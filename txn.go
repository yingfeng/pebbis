package redistore

import (
	"strings"

	"github.com/tidwall/redcon"
)

// Transactions and authentication.

// requiresAuth reports whether connections must authenticate first.
func (s *Store) requiresAuth() bool {
	return s.cfg.Load().RequirePass != ""
}

// checkPassword compares against the configured password.
func (s *Store) checkPassword(pass string) bool {
	c := s.cfg.Load()
	return c.RequirePass != "" && c.RequirePass == pass
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
	if cs.watchedExpiry == nil {
		cs.watchedExpiry = map[string]int64{}
	}
	if cs.watchedStale == nil {
		cs.watchedStale = map[string]bool{}
	}
	now := c.Store.clock.NowMilli()
	for _, a := range args {
		key := string(a)
		if _, seen := cs.watched[key]; seen {
			continue
		}
		ver := uint64(0)
		exp := int64(-1) // -1: the key did not exist at WATCH time
		stale := false
		if e, ok := c.Store.dict.Lookup(c.DB, key); ok {
			ver = e.Version()
			exp = e.Expiry()
			// Already logically expired when WATCHed?
			stale = exp > 0 && exp <= now
		}
		cs.watched[key] = ver
		cs.watchedExpiry[key] = exp
		cs.watchedStale[key] = stale
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
		cs.watchedExpiry = nil
		cs.watchedStale = nil
	}
	c.writeOK()
	return nil
}

// validateQueued checks a command before it is queued inside MULTI: Redis
// rejects unknown commands and arity violations at queue time and marks the
// transaction dirty, so that EXEC reports EXECABORT.
func validateQueued(name string, argc int) error {
	cmd, ok := commands[name]
	if !ok {
		return UnknownCommand(name)
	}
	if cmd.arity >= 0 {
		if argc != cmd.arity {
			return WrongArgs(name)
		}
	} else if argc < -cmd.arity {
		return WrongArgs(name)
	}
	return nil
}

func abortTxn(cs *connState) {
	cs.inTxn = false
	cs.queue = nil
	cs.dirtyTxn = false
	cs.watched = nil
	cs.watchedExpiry = nil
	cs.watchedStale = nil
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

	// Optimistic concurrency check, expiry-aware: a watched key that expired
	// after WATCH counts as modified, while a key that was already stale
	// (logically expired) at WATCH time is treated as non-existent, so its
	// removal does not abort the transaction.
	if len(cs.watched) > 0 {
		now := c.Store.clock.NowMilli()
		abort := false
		for key, ver := range cs.watched {
			we := int64(-1)
			if cs.watchedExpiry != nil {
				we = cs.watchedExpiry[key]
			}
			staleAtWatch := cs.watchedStale != nil && cs.watchedStale[key]
			if e, ok := c.Store.dict.Lookup(c.DB, key); ok {
				if we < 0 || e.Version() != ver {
					// Created after WATCH, or written to.
					abort = true
					break
				}
				if !staleAtWatch && e.Expiry() > 0 && e.Expiry() <= now {
					// Live when watched, expired since the WATCH without any
					// user write: expiry counts as a modification.
					abort = true
					break
				}
			} else if we >= 0 && !staleAtWatch {
				// A live key was deleted (or expired) after WATCH.
				abort = true
				break
			}
		}
		if abort {
			abortTxn(cs)
			c.writeNull()
			return nil
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
	curDB := c.DB
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
		if cm.write && c.Store.memoryFull() {
			c.w.WriteError(OOM(upper).Error())
			continue
		}
		byteArgs := make([][]byte, 0, len(line)-1)
		for _, a := range line[1:] {
			byteArgs = append(byteArgs, []byte(a))
		}
		sub := &Ctx{
			Store:      c.Store,
			DB:         curDB,
			Name:       upper,
			MaxBulkLen: c.MaxBulkLen,
			Args:       line,
			state:      cs,
			srv:        c.srv,
			noBlock:    true, // blocking commands run non-blocking inside MULTI
			w:          c.w,
		}
		if err := cm.fn(sub, byteArgs); err != nil {
			c.w.WriteError(err.Error())
		}
		// A SELECT queued in the transaction switches the database for the
		// commands after it (and for the connection afterwards), as in Redis.
		if upper == "SELECT" {
			curDB = sub.DB
		}
	}
	// The connection keeps whatever database the transaction ended on; the
	// dispatch loop mirrors c.DB back into the connection state.
	c.DB = curDB
	cs.db = curDB
	return nil
}

// compile-time check that the transaction path stays compatible with redcon's
// writer, which is what Ctx.w holds on the server path.
var _ Writer = redcon.Conn(nil)
