package memory

import (
	"math/rand/v2"
	"sync/atomic"
)

// LFU tuning, matching Redis' defaults.
const (
	// lfuInitVal is the frequency a freshly created entry starts at.
	lfuInitVal = 5
	// lfuLogFactor makes the increment steeper: the higher the counter, the
	// harder it is to raise further.
	lfuLogFactor = 10
	// lfuCounterMax is the largest representable 8-bit counter.
	lfuCounterMax = 255
)

// entryBaseSize is a rough constant covering the entry struct itself plus its
// share of the map bucket. Precision is not required; what matters is that the
// number grows and shrinks with the data so eviction tracks reality.
const entryBaseSize = 64

// entry is the in-memory index record for one key.
//
// It intentionally holds no large value: value bytes are served from Pebble's
// block cache. Only values up to InlineValueMaxSize are kept here so that the
// hot path avoids a Pebble read.
type Entry struct {
	key string
	// typ is one of the config.Type* tags.
	typ uint8
	// expireAt is unix milliseconds, or 0 when the key has no TTL.
	expireAt int64
	// lru packs a 24-bit LRU clock in the low bits and an 8-bit LFU counter in
	// the high bits. Both are mutated with atomics so that reads do not need
	// the shard lock.
	lru uint32
	// size is the estimated memory footprint of this entry.
	size uint32
	// version increments on every overwrite and lets WATCH detect a change.
	version atomic.Uint64
	// inline holds small values directly.
	inline []byte
}

// newEntry builds an entry, inlining payload when it is small enough.
func newEntry(key string, typ uint8, expireAt int64, payload []byte, inlineMax int, lruClock uint32) *Entry {
	e := &Entry{
		key:      key,
		typ:      typ,
		expireAt: expireAt,
		lru:      lruClock & lruClockMask,
	}
	if len(payload) > 0 && len(payload) <= inlineMax {
		e.inline = make([]byte, len(payload))
		copy(e.inline, payload)
	}
	e.size = entryMem(key, e.inline)
	return e
}

// entryMem estimates the memory used by one key and its inline value.
func entryMem(key string, inline []byte) uint32 {
	return uint32(entryBaseSize + len(key) + len(inline))
}

// SetExpiry records a new expiry and returns the old one.
func (e *Entry) SetExpiry(expireAt int64) int64 {
	return atomic.SwapInt64(&e.expireAt, expireAt)
}

// Expiry returns the expiry in unix milliseconds (0 means none).
func (e *Entry) Expiry() int64 {
	return atomic.LoadInt64(&e.expireAt)
}

// Expired reports whether the entry expired at or before nowMs.
func (e *Entry) Expired(nowMs int64) bool {
	exp := atomic.LoadInt64(&e.expireAt)
	return exp != 0 && exp <= nowMs
}

// Touch records an access for LRU purposes, leaving the LFU counter intact.
func (e *Entry) Touch(lruClock uint32) {
	for {
		old := atomic.LoadUint32(&e.lru)
		next := (old &^ lruClockMask) | (lruClock & lruClockMask)
		if atomic.CompareAndSwapUint32(&e.lru, old, next) {
			return
		}
	}
}

// Freq returns the LFU counter.
func (e *Entry) Freq() uint8 {
	return uint8(atomic.LoadUint32(&e.lru) >> LRUClockBits)
}

// IncrFreq bumps the LFU counter with the logarithmic probability Redis uses:
// the higher the counter, the less likely it grows. This keeps the counter
// meaningful over a long life while fitting in 8 bits.
func (e *Entry) IncrFreq() {
	for {
		old := atomic.LoadUint32(&e.lru)
		freq := old >> LRUClockBits
		if freq >= lfuCounterMax {
			return
		}
		if !shouldIncrFreq(freq) {
			return
		}
		next := old + (1 << LRUClockBits)
		if atomic.CompareAndSwapUint32(&e.lru, old, next) {
			return
		}
	}
}

// DecayFreq halves the counter, used by the periodic LFU decay pass.
func (e *Entry) DecayFreq() {
	for {
		old := atomic.LoadUint32(&e.lru)
		freq := old >> LRUClockBits
		if freq <= lfuInitVal {
			return
		}
		next := (uint32(freq/2) << LRUClockBits) | (old & lruClockMask)
		if atomic.CompareAndSwapUint32(&e.lru, old, next) {
			return
		}
	}
}

// Decay applies Redis' lazy LFU decay: for every decayMinutes worth of idle
// time since the entry was last touched, the counter drops by one. Deferring
// this to access time avoids a periodic sweep over the whole keyspace.
func (e *Entry) Decay(nowClock uint32, decayMinutes int) {
	if decayMinutes <= 0 {
		return
	}
	elapsedSec := Idle(nowClock, e.EntryClock())
	if elapsedSec == 0 {
		return
	}
	periods := int(elapsedSec) / (60 * decayMinutes)
	if periods <= 0 {
		return
	}
	for {
		old := atomic.LoadUint32(&e.lru)
		freq := old >> LRUClockBits
		if freq <= lfuInitVal {
			return
		}
		next := freq - uint32(periods)
		if next < lfuInitVal {
			next = lfuInitVal
		}
		next = (next << LRUClockBits) | (old & lruClockMask)
		if atomic.CompareAndSwapUint32(&e.lru, old, next) {
			return
		}
	}
}

// SetFreq overwrites the LFU counter; used by OBJECT FREQ's counterpart and tests.
func (e *Entry) SetFreq(freq uint8) {
	for {
		old := atomic.LoadUint32(&e.lru)
		next := (uint32(freq) << LRUClockBits) | (old & lruClockMask)
		if atomic.CompareAndSwapUint32(&e.lru, old, next) {
			return
		}
	}
}

func shouldIncrFreq(freq uint32) bool {
	if freq < lfuInitVal {
		freq = lfuInitVal
	}
	base := float64(freq - lfuInitVal)
	p := 1.0 / (base*lfuLogFactor + 1)
	return rand.Float64() < p
}

// Version returns the entry's modification counter, used by WATCH.
func (e *Entry) Version() uint64 { return e.version.Load() }

// bumpVersion advances the modification counter.
func (e *Entry) bumpVersion() uint64 { return e.version.Add(1) }

// EntryClock returns the raw 24-bit LRU clock stored on the entry.
func (e *Entry) EntryClock() uint32 {
	return atomic.LoadUint32(&e.lru) & lruClockMask
}

// Type returns the object type tag.
func (e *Entry) Type() uint8 { return e.typ }

// Inline returns the inlined value, or nil when the value is stored out of line.
func (e *Entry) Inline() []byte { return e.inline }

// Size returns the estimated memory footprint in bytes.
func (e *Entry) Size() uint32 { return e.size }

// Key returns the key this entry is indexed under.
func (e *Entry) Key() string { return e.key }
