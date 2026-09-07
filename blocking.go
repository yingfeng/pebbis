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

// blocker is a condition variable: writers bump the version on every push,
// blocked commands compare the version they last observed. Sleeping on a stale
// version cannot miss a push, no matter where the check/sleep boundary falls.

type waiter struct {
	db   uint16
	keys []string
	ch   chan struct{}
}

type blocker struct {
	mu    sync.Mutex
	seq   uint64
	chans []chan struct{}
}

func newBlocker() *blocker { return &blocker{} }

// version returns the current notify generation. Take it BEFORE checking the
// data, then sleep on it: if any push happened in between, the sleep returns
// immediately.
func (b *blocker) version() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.seq
}

// sleepSince parks until the version moves past `since` or the timeout
// elapses. The version comparison happens under the blocker lock, so a push
// concurrent with the check cannot be missed. Spurious wakeups (a push to an
// unrelated key) are fine: the caller simply re-checks its data.
func (b *blocker) sleepSince(since uint64, timeout time.Duration) bool {
	b.mu.Lock()
	if b.seq != since {
		b.mu.Unlock()
		return true
	}
	ch := make(chan struct{}, 1)
	b.chans = append(b.chans, ch)
	b.mu.Unlock()

	if timeout <= 0 {
		<-ch
		return true
	}
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		b.mu.Lock()
		for i, c := range b.chans {
			if c == ch {
				b.chans = append(b.chans[:i], b.chans[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
		return false
	}
}

// notify bumps the version and wakes every sleeper. A push to any key wakes
// all blocked commands; each re-checks its own keys, so a spurious wakeup
// costs one extra empty read and cannot lose an update.
func (b *blocker) notify(db uint16, key string) {
	b.mu.Lock()
	b.seq++
	chs := b.chans
	b.chans = nil
	b.mu.Unlock()
	for _, ch := range chs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// blockedLen reports how many commands are currently blocked, for INFO.
func (b *blocker) blockedLen() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.chans)
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

	// Version taken BEFORE the first check: a push landing anywhere between
	// the check and the sleep bumps the version and wakes us immediately.
	v := c.Store.blockVersion()
	for {
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
		if !c.Store.blockSleepSince(v, timeout) {
			c.writeNull()
			return nil
		}
		v = c.Store.blockVersion()
		// Woken up: loop and try again. A spurious wake (another client took
		// the element) simply blocks again until the deadline.
	}
}

// blockVersion is taken before the emptiness check.
func (s *Store) blockVersion() uint64 { return s.blocker.version() }

// blockSleepSince parks until a push bumps the version past `since`.
func (s *Store) blockSleepSince(since uint64, timeout time.Duration) bool {
	return s.blocker.sleepSince(since, timeout)
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

	v := c.Store.blockVersion()
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
		if !c.Store.blockSleepSince(v, timeout) {
			c.writeNull()
			return nil
		}
		v = c.Store.blockVersion()
	}
}
