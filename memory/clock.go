// Package memory implements the in-memory index, LRU/LFU metadata, expiry and
// eviction.
//
// The dict deliberately stores only metadata plus small inline values. Value
// bytes live in Pebble's block cache, which is what turns MaxMemory into a hard
// limit instead of a GC suggestion.
package memory

import (
	"sync/atomic"
	"time"
)

// lruClockResolution is the LRU clock granularity, matching Redis.
const lruClockResolution = 1000 // ms

// LRUClockBits is how many bits of the entry's lru field hold the clock. The
// remaining high bits hold the LFU counter.
const LRUClockBits = 24

// lruClockMask isolates the LRU portion of the field.
const lruClockMask = (1 << LRUClockBits) - 1 // 0x00FFFFFF

// Clock is a coarse, injectable time source.
//
// Reading the wall clock on every access would dominate the cost of a GET, so
// the value is cached and refreshed by a background ticker. Tests can Advance
// it deterministically instead of sleeping.
type Clock struct {
	cached atomic.Int64 // unix milliseconds
	offset atomic.Int64 // offset from real time, in ms (tests)
	stop   chan struct{}
	done   chan struct{}
}

// NewClock returns a clock tracking real time.
func NewClock() *Clock {
	c := &Clock{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	c.cached.Store(time.Now().UnixMilli())
	go c.run()
	return c
}

// NewStaticClock returns a pinned clock for tests. It runs no goroutine.
func NewStaticClock(t time.Time) *Clock {
	c := &Clock{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	c.cached.Store(t.UnixMilli())
	close(c.done)
	return c
}

func (c *Clock) run() {
	defer close(c.done)
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			c.cached.Store(time.Now().UnixMilli() + c.offset.Load())
		}
	}
}

// Now returns the current time.
func (c *Clock) Now() time.Time {
	return time.UnixMilli(c.NowMilli())
}

// NowMilli returns the current time as unix milliseconds.
func (c *Clock) NowMilli() int64 {
	// Re-read the real clock when an offset is applied so Advance() is visible
	// immediately rather than at the next tick.
	if off := c.offset.Load(); off != 0 {
		return time.Now().UnixMilli() + off
	}
	return c.cached.Load()
}

// Advance shifts the clock forward by d. Intended for tests.
func (c *Clock) Advance(d time.Duration) {
	c.offset.Add(d.Milliseconds())
	c.cached.Store(time.Now().UnixMilli() + c.offset.Load())
}

// LRUClock returns the current 24-bit LRU clock value.
func (c *Clock) LRUClock() uint32 {
	return uint32(c.NowMilli()/lruClockResolution) & lruClockMask
}

// Stop terminates the background refresh goroutine.
func (c *Clock) Stop() {
	select {
	case <-c.done:
		return
	default:
	}
	close(c.stop)
	<-c.done
}

// Idle returns how long ago (in LRU clock ticks) an entry was last touched,
// tolerating the 24-bit wrap-around the same way Redis does.
func Idle(nowClock, entryClock uint32) uint32 {
	nowClock &= lruClockMask
	entryClock &= lruClockMask
	if nowClock >= entryClock {
		return nowClock - entryClock
	}
	return (lruClockMask + 1) + nowClock - entryClock
}
