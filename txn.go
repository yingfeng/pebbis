package redistore

import (
	"strings"

	"github.com/tidwall/redcon"
)

// Transactions and authentication.

// requiresAuth reports whether connections must authenticate first.
func (s *Store) requiresAuth() bool {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg.RequirePass != ""
}

// checkPassword compares against the configured password.
func (s *Store) checkPassword(pass string) bool {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg.RequirePass != "" && s.cfg.RequirePass == pass
}

func cmdAuth(c *Ctx, args [][]byte) error {
	if len(args) < 1 || len(args) > 2 {
		return WrongArgs("auth")
	}
	pass := string(args[0])
	// AUTH <user> <pass> is accepted; the user is ignored, matching a server
	// without ACLs.
	if len(args) == 2 {
		pass = string(args[1])
	}
	cs := c.Client()
	if !c.Store.requiresAuth() {
		return &protoError{"ERR Client sent AUTH, but no password is set"}
	}
	if !c.Store.checkPassword(pass) {
		if cs != nil {
			cs.authed = false
		}
		return &protoError{"WRONGPASS invalid username-password pair or user is disabled."}
	}
	if cs != nil {
		cs.authed = true
	}
	c.writeOK()
	return nil
}

func cmdMulti(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	cs := c.Client()
	if cs == nil {
		return &protoError{"ERR MULTI is only available on a server connection"}
	}
	if cs.inTxn {
		return &protoError{"ERR MULTI calls can not be nested"}
	}
	cs.inTxn = true
	cs.queue = nil
	cs.dirtyTxn = false
	c.writeOK()
	return nil
}

func cmdDiscard(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	cs := c.Client()
	if cs == nil || !cs.inTxn {
		return &protoError{"ERR DISCARD without MULTI"}
	}
	abortTxn(cs)
	c.writeOK()
	return nil
}

func cmdWatch(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	cs := c.Client()
	if cs == nil {
		return &protoError{"ERR WATCH is only available on a server connection"}
	}
	if cs.inTxn {
		return &protoError{"ERR WATCH inside MULTI is not allowed"}
	}
	if cs.watched == nil {
		cs.watched = map[string]uint64{}
	}
	for _, a := range args {
		key := string(a)
		if _, seen := cs.watched[key]; seen {
			continue
		}
		ver := uint64(0)
		if e, ok := c.Store.dict.Lookup(c.DB, key); ok {
			ver = e.Version()
		}
		cs.watched[key] = ver
	}
	c.writeOK()
	return nil
}

func cmdUnwatch(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	cs := c.Client()
	if cs != nil {
		cs.watched = nil
	}
	c.writeOK()
	return nil
}

func abortTxn(cs *connState) {
	cs.inTxn = false
	cs.queue = nil
	cs.dirtyTxn = false
	cs.watched = nil
}

// cmdExec runs the queued commands.
//
// The reply is an array of the individual replies, or nil when a WATCHed key
// changed (Redis' way of reporting an aborted optimistic transaction).
func cmdExec(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	cs := c.Client()
	if cs == nil || !cs.inTxn {
		return &protoError{"ERR EXEC without MULTI"}
	}

	// Optimistic concurrency check.
	if len(cs.watched) > 0 {
		for key, ver := range cs.watched {
			cur := uint64(0)
			if e, ok := c.Store.dict.Lookup(c.DB, key); ok {
				cur = e.Version()
			}
			if cur != ver {
				abortTxn(cs)
				c.writeNull()
				return nil
			}
		}
	}
	if cs.dirtyTxn {
		abortTxn(cs)
		return &protoError{"EXECABORT Transaction discarded because of previous errors."}
	}

	queue := cs.queue
	cs.inTxn = false
	cs.queue = nil
	cs.watched = nil
	cs.dirtyTxn = false

	c.w.WriteArray(len(queue))
	for _, line := range queue {
		if len(line) == 0 {
			continue
		}
		upper := strings.ToUpper(line[0])
		cm, ok := lookupCommand(upper)
		if !ok {
			// Should not happen: the command was validated when queued.
			c.w.WriteError(UnknownCommand(strings.ToLower(line[0])).Error())
			continue
		}
		byteArgs := make([][]byte, 0, len(line)-1)
		for _, a := range line[1:] {
			byteArgs = append(byteArgs, []byte(a))
		}
		sub := &Ctx{
			Store:      c.Store,
			DB:         c.DB,
			Name:       upper,
			MaxBulkLen: c.MaxBulkLen,
			Args:       line,
			state:      cs,
			srv:        c.srv,
			w:          c.w,
		}
		if err := cm.fn(sub, byteArgs); err != nil {
			c.w.WriteError(err.Error())
		}
	}
	return nil
}

// compile-time check that the transaction path stays compatible with redcon's
// writer, which is what Ctx.w holds on the server path.
var _ Writer = redcon.Conn(nil)
