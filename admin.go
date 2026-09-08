package pebbis

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pebbis/pebbis/config"
	"github.com/pebbis/pebbis/memory"
)

// Administrative and introspection commands.

// slowEntry is one SLOWLOG record.
type slowEntry struct {
	ID        int64
	Timestamp int64 // unix seconds
	Duration  int64 // microseconds
	Args      []string
}

// SlowLog keeps a bounded ring of the slowest commands.
//
// maxLen and slowerThan are atomic: record() runs on the hot path for every
// command and must not contend with CONFIG SET, which mutates them from a
// client goroutine. The entries ring itself is guarded by mu.
type SlowLog struct {
	mu         sync.Mutex
	entries    []slowEntry
	nextID     int64
	maxLen     atomic.Int64
	slowerThan atomic.Int64 // microseconds; -1 disables, 0 logs everything
}

func newSlowLog() *SlowLog {
	sl := &SlowLog{}
	sl.maxLen.Store(128)
	sl.slowerThan.Store(10000) // 10 ms, Redis' default
	return sl
}

// record adds an entry when the command exceeded the threshold.
func (sl *SlowLog) record(d time.Duration, args []string) {
	threshold := sl.slowerThan.Load()
	if threshold < 0 {
		return
	}
	us := d.Microseconds()
	if us < threshold {
		return
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.nextID++
	sl.entries = append(sl.entries, slowEntry{
		ID:        sl.nextID,
		Timestamp: time.Now().Unix(),
		Duration:  us,
		Args:      args,
	})
	maxLen := int(sl.maxLen.Load())
	if len(sl.entries) > maxLen {
		sl.entries = sl.entries[len(sl.entries)-maxLen:]
	}
}

func (sl *SlowLog) reset() {
	sl.mu.Lock()
	sl.entries = nil
	sl.mu.Unlock()
}

func (sl *SlowLog) len() int {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return len(sl.entries)
}

// list returns up to n entries, most recent first.
func (sl *SlowLog) list(n int) []slowEntry {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if n <= 0 || n > len(sl.entries) {
		n = len(sl.entries)
	}
	out := make([]slowEntry, n)
	// Copy in reverse so the newest comes first.
	for i := range n {
		out[i] = sl.entries[len(sl.entries)-1-i]
	}
	return out
}

func cmdSlowLog(c *Ctx, args [][]byte) error {
	if len(args) == 0 {
		return WrongArgs("slowlog")
	}
	sl := c.Store.slowLog
	switch strings.ToUpper(string(args[0])) {
	case "GET":
		n := 10
		if len(args) > 1 {
			v, err := toInt64(args[1])
			if err != nil {
				return err
			}
			n = int(v)
		}
		entries := sl.list(n)
		c.w.WriteArray(len(entries))
		for _, e := range entries {
			c.w.WriteArray(4)
			c.writeInt(e.ID)
			writeInt(c, e.Timestamp)
			writeInt(c, e.Duration)
			c.w.WriteArray(len(e.Args))
			for _, a := range e.Args {
				c.w.WriteBulkString(a)
			}
		}
		return nil
	case "LEN":
		c.writeInt(int64(sl.len()))
		return nil
	case "RESET":
		sl.reset()
		c.writeOK()
		return nil
	default:
		return ErrSyntax
	}
}

func writeInt(c *Ctx, n int64) { c.w.WriteInt64(n) }

// cmdClient implements the subset of CLIENT that matters operationally:
// ID, INFO, LIST, SETNAME, GETNAME. KILL and PAUSE need connection registry
// support that arrives with the PubSub work.
func cmdClient(c *Ctx, args [][]byte) error {
	if len(args) == 0 {
		return WrongArgs("client")
	}
	cs := c.Client()
	if cs == nil {
		return &protoError{"ERR CLIENT is only available on a server connection"}
	}
	switch strings.ToUpper(string(args[0])) {
	case "ID":
		c.writeInt(cs.ID)
	case "SETNAME":
		if len(args) != 2 {
			return WrongArgs("client")
		}
		name := string(args[1])
		// Redis rejects names with spaces, newlines or other control chars.
		if strings.ContainsAny(name, " \n\r\t") {
			return &protoError{"ERR Client names cannot contain spaces, newlines or special characters."}
		}
		cs.Name = name
		c.writeOK()
	case "GETNAME":
		if len(args) != 1 {
			return WrongArgs("client")
		}
		if cs.Name == "" {
			c.writeNull()
		} else {
			c.w.WriteBulkString(cs.Name)
		}
	case "INFO":
		c.w.WriteBulkString(clientInfo(c, cs))
	case "LIST":
		c.w.WriteBulkString(clientInfo(c, cs))
	case "SETINFO":
		// Go-redis sends CLIENT SETINFO (lib-name/lib-ver) on every new
		// connection. The attribute is accepted and discarded; Redis itself
		// only rejects the reserved attributes.
		if len(args) != 3 {
			return WrongArgs("client")
		}
		attr := strings.ToLower(string(args[1]))
		if attr == "lib-ver" || attr == "lib-name" {
			// reserved but settable in Redis 7.2; we just accept it.
			c.writeOK()
			return nil
		}
		return &protoError{"ERR Unrecognized option specified by CLIENT SETINFO"}
	default:
		return &protoError{"ERR unknown subcommand '" + strings.ToLower(string(args[0])) + "'. Try CLIENT HELP."}
	}
	return nil
}

func clientInfo(c *Ctx, cs *connState) string {
	var b strings.Builder
	b.WriteString("id=")
	b.WriteString(strconv.FormatInt(cs.ID, 10))
	b.WriteString(" addr=")
	b.WriteString(cs.Addr)
	b.WriteString(" db=")
	b.WriteString(strconv.Itoa(int(c.DB)))
	b.WriteString(" name=")
	b.WriteString(cs.Name)
	b.WriteString("\n")
	return b.String()
}

// cmdObject reports encoding and eviction metadata for a key.
func cmdObject(c *Ctx, args [][]byte) error {
	if len(args) < 1 {
		return WrongArgs("object")
	}
	if len(args) != 2 {
		return WrongArgs("object|idletime")
	}
	key := string(args[1])
	switch strings.ToUpper(string(args[0])) {
	case "ENCODING":
		typ, ok, err := c.Store.typeOf(c.DB, key)
		if err != nil {
			return err
		}
		if !ok {
			c.writeNull()
			return nil
		}
		if typ == config.TypeString {
			c.w.WriteBulkString("embstr")
		} else {
			c.w.WriteBulkString("listpack")
		}
		return nil
	case "FREQ":
		tracksLFU := c.Store.cfg.Load().TracksLFU()
		if !tracksLFU {
			return &protoError{"ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust."}
		}
		e, ok := c.Store.dict.Lookup(c.DB, key)
		if !ok {
			c.writeNull()
			return nil
		}
		c.writeInt(int64(e.Freq()))
		return nil
	case "IDLETIME":
		// Real Redis tracks access time regardless of the active policy, so
		// no policy guard here; an untracked key simply reads as just-touched.
		e, ok := c.Store.dict.Lookup(c.DB, key)
		if !ok {
			c.writeNull()
			return nil
		}
		c.writeInt(int64(memory.Idle(c.Store.clock.LRUClock(), e.EntryClock())))
		return nil
	case "REFCOUNT":
		c.writeInt(1)
		return nil
	default:
		return &protoError{"ERR unknown subcommand '" + strings.ToLower(string(args[0])) + "'. Try OBJECT HELP."}
	}
}

// cmdDebug implements the DEBUG subcommands Pebbis supports. Only
// SET-ACTIVE-EXPIRE exists: it toggles the background expiry cycle, mirroring
// Redis' switch used by tests to create stale (logically expired) keys.
func cmdDebug(c *Ctx, args [][]byte) error {
	if len(args) < 1 {
		return WrongArgs("debug")
	}
	switch strings.ToUpper(string(args[0])) {
	case "SET-ACTIVE-EXPIRE":
		if len(args) != 2 {
			return WrongArgs("debug set-active-expire")
		}
		v, err := atoi(args[1])
		if err != nil || (v != 0 && v != 1) {
			return ErrSyntax
		}
		c.Store.expirer.SetEnabled(v == 1)
		c.writeOK()
		return nil
	default:
		return &protoError{"ERR DEBUG subcommand '" + strings.ToUpper(string(args[0])) + "' is not supported"}
	}
}

// cmdConfigSet applies the runtime-tunable subset of CONFIG. Anything that
// would change on-disk layout or the shard topology is rejected: it would need
// a restart, and pretending otherwise would be worse than saying no.
func cmdConfigSet(c *Ctx, name string, value string) error {
	s := c.Store
	next := *s.cfg.Load()
	switch strings.ToLower(name) {
	case "maxmemory":
		v, err := ParseByteSize(value)
		if err != nil {
			return err
		}
		next.MaxMemory = v
	case "maxmemory-policy":
		next.EvictionPolicy = parseEvictionPolicy(strings.ToLower(value))
		if next.EvictionPolicy == "" {
			return &protoError{"ERR Unsupported maxmemory-policy"}
		}
	case "maxmemory-samples":
		v, err := strconv.Atoi(value)
		if err != nil || v <= 0 {
			return &protoError{"ERR Invalid maxmemory-samples value"}
		}
		next.EvictionSample = v
	case "lfu-decay-time":
		v, err := strconv.Atoi(value)
		if err != nil || v < 0 {
			return &protoError{"ERR Invalid lfu-decay-time value"}
		}
		next.LFUDecayMinutes = v
	case "slowlog-log-slower-than":
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return &protoError{"ERR Invalid slowlog-log-slower-than value"}
		}
		s.slowLog.slowerThan.Store(v)
	case "slowlog-max-len":
		v, err := strconv.Atoi(value)
		if err != nil || v <= 0 {
			return &protoError{"ERR Invalid slowlog-max-len value"}
		}
		s.slowLog.maxLen.Store(int64(v))
	case "lua-time-limit":
		v, err := strconv.Atoi(value)
		if err != nil || v < 0 {
			return &protoError{"ERR Invalid lua-time-limit value"}
		}
		next.LuaTimeLimit = v
	default:
		return &protoError{"ERR CONFIG SET is not supported for this parameter"}
	}
	s.cfg.Store(&next)
	c.writeOK()
	return nil
}

// ParseByteSize accepts a plain byte count or a 1k/1mb/1gb suffix.
// Exported so the command-line entry point can accept the same syntax as
// CONFIG SET maxmemory.
func ParseByteSize(s string) (uint64, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, &protoError{"ERR Invalid maxmemory value"}
	}
	mult := uint64(1)
	switch {
	case strings.HasSuffix(s, "gb"):
		mult, s = 1<<30, strings.TrimSuffix(s, "gb")
	case strings.HasSuffix(s, "mb"):
		mult, s = 1<<20, strings.TrimSuffix(s, "mb")
	case strings.HasSuffix(s, "kb"):
		mult, s = 1<<10, strings.TrimSuffix(s, "kb")
	}
	v, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, &protoError{"ERR Invalid maxmemory value"}
	}
	return v * mult, nil
}

// EvictionPolicyType validates an eviction policy name.
func parseEvictionPolicy(v string) config.EvictionPolicy {
	switch config.EvictionPolicy(v) {
	case config.NoEviction, config.AllKeysLRU, config.AllKeysRandom,
		config.VolatileLRU, config.VolatileRandom, config.VolatileTTL,
		config.AllKeysLFU, config.VolatileLFU:
		return config.EvictionPolicy(v)
	}
	return ""
}

func cmdShutdown(c *Ctx, args [][]byte) error {
	if len(args) > 1 {
		return ErrSyntax
	}
	// Flush memtables so a restart needs no WAL recovery.
	if err := c.Store.Flush(); err != nil {
		return err
	}
	c.Store.requestShutdown()
	c.writeNull()
	return nil
}

// cmdInfoClients fills in the connection count for the clients section.
func cmdInfoClients(c *Ctx) string {
	var b strings.Builder
	b.WriteString("# Clients\r\n")
	writeIntField(&b, "connected_clients", c.Server().ConnectedClients())
	return b.String()
}

func writeIntField(b *strings.Builder, name string, v int64) {
	b.WriteString(name)
	b.WriteString(":")
	b.WriteString(strconv.FormatInt(v, 10))
	b.WriteString("\r\n")
}

// sortedStringKeys is a small helper for deterministic INFO-like output.
func sortedStringKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
