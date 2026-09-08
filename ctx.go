package redistore

import (
	"time"

	"github.com/tidwall/redcon"
)

// Writer is the RESP response surface a command handler may use.
//
// redcon.Conn satisfies it directly, so the RESP path allocates nothing extra,
// while tests can substitute a recorder.
type Writer interface {
	WriteString(str string)
	WriteBulk(bulk []byte)
	WriteBulkString(bulk string)
	WriteInt(num int)
	WriteInt64(num int64)
	WriteArray(count int)
	WriteNull()
	WriteError(msg string)
}

// Ctx is the per-command execution context.
type Ctx struct {
	// Store is the instance being served.
	Store *Store
	// DB is the logical database selected by this connection.
	DB uint16
	// Name is the upper-cased command name, used in error messages.
	Name string
	// MaxBulkLen caps a single argument, guarding against a huge SET
	// exhausting memory before anything is written.
	MaxBulkLen int
	// Args is the full command line, kept for the slow log.
	Args []string

	// state is the per-connection state, nil for embedded use.
	state *connState
	// srv is the serving Server, nil for embedded use.
	srv *Server
	// conn is the underlying client connection, needed by commands that detach
	// it (SUBSCRIBE) or block on it (BLPOP).
	conn redcon.Conn

	// noBlock marks execution inside MULTI/EXEC: blocking commands must behave
	// like their non-blocking forms and reply nil on empty keys instead of
	// parking the connection (Redis semantics).
	noBlock bool

	w Writer
}

// blockSleep parks the connection until a push bumps the version past since.
// In a MULTI/EXEC context it returns false immediately, so the caller writes a
// nil reply instead of blocking.
func (c *Ctx) blockSleep(since uint64, timeout time.Duration) bool {
	if c.noBlock {
		return false
	}
	return c.Store.blockSleepSince(since, timeout)
}

// Client returns the per-connection state, or nil when the command did not come
// from a network connection.
func (c *Ctx) Client() *connState { return c.state }

// Server returns the serving Server, or nil when not served over the network.
func (c *Ctx) Server() *Server { return c.srv }

// pubsubTarget returns the Server and connection when a command can operate on
// subscriptions; both are nil in embedded use.
func (c *Ctx) pubsubTarget() (*Server, redcon.Conn) {
	if c.srv == nil || c.conn == nil {
		return nil, nil
	}
	return c.srv, c.conn
}

// write helpers keep handlers terse without hiding the writer.
func (c *Ctx) writeOK()         { c.w.WriteString("OK") }
func (c *Ctx) writeInt(n int64) { c.w.WriteInt64(n) }
func (c *Ctx) writeBulk(b []byte) {
	if b == nil {
		c.w.WriteNull()
		return
	}
	c.w.WriteBulk(b)
}
func (c *Ctx) writeNull()         { c.w.WriteNull() }
func (c *Ctx) writeErr(err error) { c.w.WriteError(err.Error()) }

// checkArgLen validates the argument count against arity.
// A negative arity means "at least |arity| arguments", Redis' convention.
func (c *Ctx) checkArgLen(n, arity int) error {
	if arity >= 0 {
		if n != arity {
			return WrongArgs(c.Name)
		}
		return nil
	}
	if n < -arity {
		return WrongArgs(c.Name)
	}
	return nil
}

// checkBulkLen enforces the per-argument size cap.
func (c *Ctx) checkBulkLen(args ...[]byte) error {
	if c.MaxBulkLen <= 0 {
		return nil
	}
	for _, a := range args {
		if len(a) > c.MaxBulkLen {
			return errTooBig
		}
	}
	return nil
}

// errTooBig is returned when an argument exceeds MaxBulkLen.
var errTooBig = &protoError{"ERR string exceeds maximum allowed size (proto-max-bulk-len)"}

// protoError is a pre-formatted Redis error.
type protoError struct{ msg string }

func (e *protoError) Error() string { return e.msg }
