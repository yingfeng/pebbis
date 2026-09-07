package redistore

import (
	"sync"
	"time"
)

// Blocking list commands.
//
// Redis runs on a single thread and therefore cannot block in a command, which
// is why it keeps a per-key list of blocked clients and serves them from the
// event loop. Here every connection already has its own goroutine, so the
// command simply waits: no detached-connection bookkeeping, no special
// resumption path. The waiter table exists only so a push can wake a sleeper
// instead of making it wait out its timeout.

// waiter is one blocked command.
type waiter struct {
	db   uint16
	keys []string
	ch   chan struct{}
}

// blocker wakes commands blocked on BLPOP/BRPOP when a list gains an element.
type blocker struct {
	mu      sync.Mutex
	waiters []*waiter
}

func newBlocker() *blocker { return &blocker{} }

// wait blocks until one of keys is signalled or the timeout elapses. A zero or
// negative timeout waits forever, matching Redis' "0 = block indefinitely".
func (b *blocker) wait(db uint16, keys []string, timeout time.Duration) bool {
	w := &waiter{db: db, keys: keys, ch: make(chan struct{}, 1)}

	b.mu.Lock()
	b.waiters = append(b.waiters, w)
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		for i, x := range b.waiters {
			if x == w {
				b.waiters = append(b.waiters[:i], b.waiters[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
	}()

	if timeout <= 0 {
		<-w.ch
		return true
	}
	select {
	case <-w.ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

// notify wakes every waiter interested in (db, key).
func (b *blocker) notify(db uint16, key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, w := range b.waiters {
		if w.db != db {
			continue
		}
		for _, k := range w.keys {
			if k != key {
				continue
			}
			// Buffered with capacity 1: a signal arriving with nobody waiting
			// is not lost, it just makes the next wait return immediately.
			select {
			case w.ch <- struct{}{}:
			default:
			}
			break
		}
	}
}

// blockedLen reports how many commands are currently blocked, for INFO.
func (b *blocker) blockedLen() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.waiters)
}

// blockedPop is the shared body of BLPOP and BRPOP.
func blockedPop(c *Ctx, args [][]byte, left bool) error {
	if len(args) < 2 {
		return WrongArgs("blpop")
	}
	// Redis parses the timeout as an integer number of seconds; a fractional
	// value is rejected rather than rounded.
	secs, err := toInt64(args[len(args)-1])
	if err != nil {
		return err
	}
	if secs < 0 {
		return &protoError{"ERR timeout is negative"}
	}
	keys := make([]string, 0, len(args)-1)
	for _, a := range args[:len(args)-1] {
		keys = append(keys, string(a))
	}

	timeout := time.Duration(secs) * time.Second
	deadline := time.Time{}
	if secs > 0 {
		deadline = time.Now().Add(timeout)
	}

	for {
		// Try every key in order before blocking: a non-empty list is served
		// immediately, exactly as Redis does.
		for _, k := range keys {
			out, err := c.Store.listPop(c.DB, k, 1, left)
			if err != nil {
				return err
			}
			if len(out) == 0 {
				continue
			}
			c.w.WriteArray(2)
			c.w.WriteBulkString(k)
			c.w.WriteBulkString(out[0])
			return nil
		}

		if secs > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				c.writeNull()
				return nil
			}
			timeout = remaining
		}
		if !c.Store.blockWait(c.DB, keys, timeout) {
			c.writeNull()
			return nil
		}
		// Woken up: loop and try again. A spurious wake (another client took
		// the element) simply blocks again until the deadline.
	}
}

// blockWait is the Store-level entry used by the blocking commands.
func (s *Store) blockWait(db uint16, keys []string, timeout time.Duration) bool {
	return s.blocker.wait(db, keys, timeout)
}

// notifyListChanged wakes anyone blocked on key. Called after a successful push.
func (s *Store) notifyListChanged(db uint16, key string) {
	s.blocker.notify(db, key)
}

func cmdBLPop(c *Ctx, args [][]byte) error { return blockedPop(c, args, true) }
func cmdBRPop(c *Ctx, args [][]byte) error { return blockedPop(c, args, false) }

// cmdBRPopLPush blocks on source and, once an element is available, moves it to
// destination. It is the blocking form of RPOPLPUSH.
func cmdBRPopLPush(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	secs, err := toInt64(args[2])
	if err != nil {
		return err
	}
	if secs < 0 {
		return &protoError{"ERR timeout is negative"}
	}
	src, dst := string(args[0]), string(args[1])

	timeout := time.Duration(secs) * time.Second
	deadline := time.Time{}
	if secs > 0 {
		deadline = time.Now().Add(timeout)
	}

	for {
		out, err := c.Store.listPop(c.DB, src, 1, false)
		if err != nil {
			return err
		}
		if len(out) > 0 {
			if _, err := c.Store.listPush(c.DB, dst, []string{out[0]}, true, false); err != nil {
				return err
			}
			c.w.WriteBulkString(out[0])
			return nil
		}
		if secs > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				c.writeNull()
				return nil
			}
			timeout = remaining
		}
		if !c.Store.blockWait(c.DB, []string{src}, timeout) {
			c.writeNull()
			return nil
		}
	}
}
