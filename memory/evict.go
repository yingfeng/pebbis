package memory

import (
	"sync/atomic"
	"time"

	"github.com/redistore/redistore/config"
)

// candidate is one sampled eviction victim.
type candidate struct {
	db       uint16
	key      string
	size     int64
	idle     uint32 // LRU ticks since last access
	freq     uint8  // LFU counter
	expireAt int64  // unix ms, 0 when volatile
}

// Evictor keeps the dict under MaxMemory.
//
// Sampling rather than maintaining an exact LRU list is deliberate: it costs
// nothing on the read path and, with a handful of samples per round, lands
// within a few percent of true LRU hit rate on realistic workloads.
type Evictor struct {
	cfg  *config.Config
	dict *Dict
	// deleteFn removes the persisted copy. It is only invoked in
	// config.EvictCache mode; EvictPersist leaves the data on disk.
	deleteFn func(db uint16, key string) error

	evicted atomic.Int64
}

// NewEvictor builds an evictor over dict.
func NewEvictor(cfg *config.Config, dict *Dict, deleteFn func(db uint16, key string) error) *Evictor {
	if deleteFn == nil {
		deleteFn = func(uint16, string) error { return nil }
	}
	return &Evictor{cfg: cfg, dict: dict, deleteFn: deleteFn}
}

// Evicted returns the cumulative number of keys evicted.
func (ev *Evictor) Evicted() int64 { return ev.evicted.Load() }

// MaybeEvict evicts a bounded number of keys when the budget is exceeded.
// It is called on the write path, so it must stay cheap: it returns as soon as
// usage is back under target or the round limit is hit.
func (ev *Evictor) MaybeEvict() {
	if !ev.cfg.EvictionEnabled() {
		return
	}
	target := int64(float64(ev.cfg.MaxMemory) * ev.cfg.EvictionTargetRatio)
	if ev.dict.Used() <= target {
		return
	}

	// Cap a single round so a large write cannot block the command for long.
	const roundLimit = 64
	deadline := time.Now().Add(time.Millisecond)
	for range roundLimit {
		if ev.dict.Used() <= target || time.Now().After(deadline) {
			return
		}
		if !ev.evictOne() {
			return // nothing evictable
		}
	}
}

// Run drives background eviction until stop is closed.
func (ev *Evictor) Run(stop <-chan struct{}) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			ev.MaybeEvict()
		}
	}
}

// evictOne samples the policy's worth of keys and removes the best victim.
func (ev *Evictor) evictOne() bool {
	c, ok := ev.pick()
	if !ok {
		return false
	}
	if ev.cfg.EvictionMode == config.EvictCache {
		if err := ev.deleteFn(c.db, c.key); err != nil {
			return false
		}
	}
	if _, ok := ev.dict.Delete(c.db, c.key); !ok {
		return false
	}
	ev.evicted.Add(1)
	return true
}

// pick returns the best victim among a random sample.
func (ev *Evictor) pick() (candidate, bool) {
	samples := ev.sample(ev.cfg.EvictionSample)
	if len(samples) == 0 {
		return candidate{}, false
	}
	best := samples[0]
	switch ev.cfg.EvictionPolicy {
	case config.AllKeysLRU, config.VolatileLRU:
		for _, c := range samples[1:] {
			if c.idle > best.idle {
				best = c
			}
		}
	case config.AllKeysLFU, config.VolatileLFU:
		for _, c := range samples[1:] {
			if c.freq < best.freq {
				best = c
			}
		}
	case config.VolatileTTL:
		for _, c := range samples[1:] {
			if c.expireAt != 0 && (best.expireAt == 0 || c.expireAt < best.expireAt) {
				best = c
			}
		}
	case config.AllKeysRandom, config.VolatileRandom:
		return best, true // any sample will do
	}
	return best, true
}

// sample collects up to n random candidates.
//
// Go randomises the starting point of a map range, so taking the first matching
// entry of a randomly chosen shard is an unbiased sample - no auxiliary
// key list needed.
func (ev *Evictor) sample(n int) []candidate {
	if n <= 0 {
		n = 5
	}
	nowMs := ev.dict.clock.NowMilli()
	nowClock := ev.dict.clock.LRUClock()
	volatileOnly := ev.cfg.VolatileOnly()

	out := make([]candidate, 0, n)
	for db := range ev.dict.dbCount {
		ds := ev.dict.dbs[db].Load()
		if ds == nil {
			continue
		}
		for range n {
			// Draw from the shards that are known to hold keys: with far more
			// shards than keys, a uniform draw would almost always land on an
			// empty one and eviction would make no progress.
			s := ds.randomShard()
			if s == nil {
				break
			}
			if c, ok := pickFromShard(s, uint16(db), nowMs, nowClock, volatileOnly); ok {
				out = append(out, c)
			}
		}
	}
	return out
}

func pickFromShard(s *shard, db uint16, nowMs int64, nowClock uint32, volatileOnly bool) (candidate, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for key, e := range s.m {
		if e.Expired(nowMs) {
			// Expired keys are reaped by the expiry cycle; prefer them anyway
			// since removing one is always progress.
			return candidate{db: db, key: key, size: int64(e.size), idle: ^uint32(0), expireAt: e.Expiry()}, true
		}
		if volatileOnly && e.Expiry() == 0 {
			continue
		}
		return candidate{
			db:       db,
			key:      key,
			size:     int64(e.size),
			idle:     Idle(nowClock, e.EntryClock()),
			freq:     e.Freq(),
			expireAt: e.Expiry(),
		}, true
	}
	return candidate{}, false
}
