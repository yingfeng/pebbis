package redistore

import (
	"crypto/tls"
	"errors"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/glob"
	"github.com/tidwall/redcon"
)

// ServerOptions configures the RESP listener.
type ServerOptions struct {
	// Addr is the listen address, e.g. ":6379".
	Addr string
	// Network defaults to "tcp"; "unix" is also supported.
	Network string
	// TLSConfig enables TLS when set (and mTLS when ClientAuth is set).
	TLSConfig *tls.Config
	// MaxClients caps concurrent connections. Zero means unlimited.
	MaxClients int
	// IdleTimeout closes connections with no traffic for this long.
	IdleTimeout time.Duration
	// MaxBulkLen caps a single command argument, guarding against a huge SET
	// exhausting memory before anything is written.
	MaxBulkLen int
	// MaxClients 之外的背压：单条命令响应的写缓冲上限。
	MaxWriteBuffer int
}

// defaultMaxBulkLen matches Redis' proto-max-bulk-len default of 512 MB.
const defaultMaxBulkLen = 512 << 20

// Server serves the RESP protocol over a Store.
//
// The protocol and connection handling come from redcon: it parses an entire
// pipeline in one read and flushes the batch of replies in one write, which is
// the main reason it was picked over rolling our own loop.
type Server struct {
	store *Store
	opts  ServerOptions

	srv  *redcon.Server
	tsrv *redcon.TLSServer

	mu         sync.Mutex
	conns      atomic.Int64
	nextConnID atomic.Int64
	sem        chan struct{}

	// Pub/Sub registry. redcon owns the detached connections; the maps below
	// are our own counters, kept so PUBSUB CHANNELS / NUMSUB / NUMPAT have
	// something to report.
	ps       redcon.PubSub
	psMu     sync.Mutex
	channels map[string]int
	patterns map[string]int
}

// subscribe registers a subscriber and hands the connection to redcon, which
// detaches it and answers subsequent subscriber-mode commands itself.
func (srv *Server) subscribe(conn redcon.Conn, channel string, pattern bool) {
	srv.psMu.Lock()
	if pattern {
		if srv.patterns == nil {
			srv.patterns = map[string]int{}
		}
		srv.patterns[channel]++
	} else {
		if srv.channels == nil {
			srv.channels = map[string]int{}
		}
		srv.channels[channel]++
	}
	srv.psMu.Unlock()

	if pattern {
		srv.ps.Psubscribe(conn, channel)
	} else {
		srv.ps.Subscribe(conn, channel)
	}
}

func (srv *Server) unsubscribeCount(channel string, pattern bool) {
	srv.psMu.Lock()
	defer srv.psMu.Unlock()
	m := srv.channels
	if pattern {
		m = srv.patterns
	}
	if n := m[channel]; n > 1 {
		m[channel] = n - 1
	} else {
		delete(m, channel)
	}
}

// channelNames returns the active channels, optionally filtered by a glob.
func (srv *Server) channelNames(pattern string) []string {
	srv.psMu.Lock()
	defer srv.psMu.Unlock()
	out := make([]string, 0, len(srv.channels))
	for ch := range srv.channels {
		if pattern != "" && !globMatch(pattern, ch) {
			continue
		}
		out = append(out, ch)
	}
	sort.Strings(out)
	return out
}

// chanCount is one PUBSUB NUMSUB entry.
type chanCount struct {
	name string
	n    int
}

// channelCounts returns subscriber counts in argument order.
func (srv *Server) channelCounts(channels []string) []chanCount {
	srv.psMu.Lock()
	defer srv.psMu.Unlock()
	out := make([]chanCount, 0, len(channels))
	for _, ch := range channels {
		out = append(out, chanCount{ch, srv.channels[ch]})
	}
	return out
}

func (srv *Server) patternCount() int {
	srv.psMu.Lock()
	defer srv.psMu.Unlock()
	return len(srv.patterns)
}

// globMatch implements Redis' PUBSUB CHANNELS pattern, a simple
// * / ? / [...] glob.
func globMatch(pattern, s string) bool {
	if pattern == "" || pattern == "*" {
		return true
	}
	g, err := glob.Compile(pattern)
	if err != nil {
		return pattern == s
	}
	return g.Match(s)
}

// NewServer wraps a Store with a RESP listener.
func NewServer(s *Store, opts ServerOptions) *Server {
	if opts.Addr == "" {
		opts.Addr = ":6379"
	}
	if opts.Network == "" {
		opts.Network = "tcp"
	}
	if opts.MaxBulkLen <= 0 {
		opts.MaxBulkLen = defaultMaxBulkLen
	}
	srv := &Server{store: s, opts: opts}
	if opts.MaxClients > 0 {
		srv.sem = make(chan struct{}, opts.MaxClients)
	}

	handler := func(conn redcon.Conn, cmd redcon.Command) { srv.handle(conn, cmd) }
	accept := func(conn redcon.Conn) bool {
		if srv.sem == nil {
			srv.conns.Add(1)
			return true
		}
		select {
		case srv.sem <- struct{}{}:
			srv.conns.Add(1)
			return true
		default:
			conn.WriteError("ERR max number of clients reached")
			return false
		}
	}
	closed := func(conn redcon.Conn, err error) {
		srv.conns.Add(-1)
		if srv.sem != nil {
			select {
			case <-srv.sem:
			default:
			}
		}
	}

	if opts.TLSConfig != nil {
		srv.tsrv = redcon.NewServerNetworkTLS(
			opts.Network, opts.Addr, handler, accept, closed, opts.TLSConfig)
	} else {
		srv.srv = redcon.NewServerNetwork(
			opts.Network, opts.Addr, handler, accept, closed)
	}
	return srv
}

// ListenAndServe binds opts.Addr and serves. It blocks until Close is called or
// the listener fails.
func (srv *Server) ListenAndServe() error {
	if srv.opts.IdleTimeout > 0 && srv.srv != nil {
		srv.srv.SetIdleClose(srv.opts.IdleTimeout)
	}
	if srv.tsrv != nil {
		return srv.tsrv.ListenAndServe()
	}
	return srv.srv.ListenAndServe()
}

// Serve accepts connections on an existing listener. Useful when the address
// must be known before the server starts, as in tests that bind port 0.
func (srv *Server) Serve(ln net.Listener) error {
	if srv.opts.IdleTimeout > 0 && srv.srv != nil {
		srv.srv.SetIdleClose(srv.opts.IdleTimeout)
	}
	if srv.tsrv != nil {
		return srv.tsrv.Serve(ln)
	}
	return srv.srv.Serve(ln)
}

func (srv *Server) setIdleClose(d time.Duration) {
	if srv.srv != nil {
		srv.srv.SetIdleClose(d)
	}
}

// Close stops the listener.
func (srv *Server) Close() error {
	if srv.tsrv != nil {
		return srv.tsrv.Close()
	}
	if srv.srv != nil {
		return srv.srv.Close()
	}
	return errors.New("redistore: server not started")
}

// Addr returns the listener address, once ListenAndServe has bound it.
func (srv *Server) Addr() string { return srv.opts.Addr }

// ConnectedClients returns the current connection count.
func (srv *Server) ConnectedClients() int64 { return srv.conns.Load() }

// handle dispatches one command. It runs on the connection's own goroutine, so
// the per-connection state needs no locking.
func (srv *Server) handle(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 0 {
		return
	}
	srv.store.stats.commands.Add(1)

	name := string(cmd.Args[0])
	args := cmd.Args[1:]
	upper := strings.ToUpper(name)

	cm, ok := lookupCommand(upper)
	if !ok {
		// Redis: an unknown command inside MULTI poisons the queue, so EXEC
		// replies EXECABORT and discards everything queued so far.
		if st := srv.connState(conn); st.inTxn {
			st.dirtyTxn = true
		}
		conn.WriteError(UnknownCommand(strings.ToLower(name)).Error())
		return
	}

	st := srv.connState(conn)

	// Reject everything but AUTH until the connection is authenticated.
	if !st.authed && srv.store.requiresAuth() && upper != "AUTH" {
		conn.WriteError("NOAUTH Authentication required")
		return
	}

	// Inside MULTI, only the transaction control commands run; everything else
	// is queued and answered with +QUEUED. WATCH is deliberately excluded: it
	// must fail loudly instead of being queued.
	if st.inTxn && upper != "MULTI" && upper != "EXEC" && upper != "DISCARD" && upper != "QUIT" && upper != "WATCH" {
		st.queue = append(st.queue, flattenArgs(name, args))
		conn.WriteString("QUEUED")
		return
	}

	full := flattenArgs(name, args)
	c := &Ctx{
		Store:      srv.store,
		DB:         st.db,
		Name:       upper,
		MaxBulkLen: srv.opts.MaxBulkLen,
		Args:       full,
		state:      st,
		srv:        srv,
		conn:       conn,
		w:          conn,
	}

	start := time.Now()
	err := cm.fn(c, args)
	srv.store.slowLog.record(time.Since(start), full)
	if err != nil {
		if IsQuit(err) {
			conn.Close()
			return
		}
		conn.WriteError(err.Error())
		return
	}
	// SELECT mutates the context; persist it for subsequent commands.
	st.db = c.DB
}

// flattenArgs joins the command name and its arguments into one slice for the
// slow log. The strings share no memory with redcon's read buffer.
func flattenArgs(name string, args [][]byte) []string {
	out := make([]string, 0, 1+len(args))
	out = append(out, name)
	for _, a := range args {
		out = append(out, string(a))
	}
	return out
}

// connState is the mutable per-connection state. It is touched only from the
// connection's own goroutine, so it needs no locking.
type connState struct {
	ID     int64
	Addr   string
	Name   string
	db     uint16
	authed bool

	// Transaction state.
	inTxn    bool
	queue    [][]string
	watched  map[string]uint64
	dirtyTxn bool // a queued command failed to parse
}

func (srv *Server) connState(conn redcon.Conn) *connState {
	if v := conn.Context(); v != nil {
		if st, ok := v.(*connState); ok {
			return st
		}
	}
	st := &connState{
		ID:     srv.nextConnID.Add(1),
		Addr:   conn.RemoteAddr(),
		authed: !srv.store.requiresAuth(),
	}
	conn.SetContext(st)
	return st
}
