package memory

import (
	"sync/atomic"
	"time"

	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/storage"
)

// Expirer reaps keys whose TTL has passed.
//
// Redis samples its expires dict at random and hopes to find due keys. The
// expiry segment here is keyed by expiry time, so a single ordered prefix scan
// yields exactly the due keys - no sampling, no wasted work, and no risk of
// leaving expired keys behind when the keyspace is large.
type Expirer struct {
	eng   *storage.Engine
	dict  *Dict
	clock *Clock
	cfg   *config.Config

	expired atomic.Int64
	// enabled toggles the active expiry cycle (DEBUG SET-ACTIVE-EXPIRE).
	// Lazy expiry on access is unaffected, matching Redis.
	enabled atomic.Bool
}

// NewExpirer builds an expirer over eng and dict.
func NewExpirer(eng *storage.Engine, dict *Dict, clock *Clock, cfg *config.Config) *Expirer {
	x := &Expirer{eng: eng, dict: dict, clock: clock, cfg: cfg}
	x.enabled.Store(true)
	return x
}

// SetEnabled turns the active expiry cycle on or off (DEBUG SET-ACTIVE-EXPIRE).
func (x *Expirer) SetEnabled(on bool) { x.enabled.Store(on) }

// Expired returns the cumulative number of keys reaped.
func (x *Expirer) Expired() int64 { return x.expired.Load() }

// Run drives the active expiry cycle until stop is closed.
func (x *Expirer) Run(stop <-chan struct{}) {
	t := time.NewTicker(x.cfg.ExpireCycleInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if !x.enabled.Load() {
				continue
			}
			if _, err := x.Cycle(); err != nil {
				return
			}
		}
	}
}

// Cycle removes up to cfg.ExpireBatch due keys, returning how many were
// removed. It stops early once the cycle budget is exhausted so that expiry
// never monopolises a CPU.
func (x *Expirer) Cycle() (int, error) {
	now := x.clock.NowMilli()
	deadline := time.Now().Add(x.cfg.ExpireCycleBudget)

	batch := x.eng.Batch()
	defer batch.Close()

	collected := 0
	err := x.eng.ScanRange(storage.ExpirePrefix(), storage.ExpireUpperBound(),
		func(k, _ []byte) error {
			expireAt, db, key, ok := storage.DecodeExpireKey(k)
			if !ok {
				return nil
			}
			// The segment is ordered by expiry: the first not-due key ends the scan.
			if expireAt > now {
				return storage.ErrStop
			}
			if !x.dict.Valid(db) {
				return nil
			}
			if err := batch.Delete(storage.EncodeDataKey(db, key)); err != nil {
				return err
			}
			// k is only valid during iteration, so it must be copied.
			if err := batch.Delete(append([]byte(nil), k...)); err != nil {
				return err
			}
			x.dict.Delete(db, key)
			collected++
			if collected >= x.cfg.ExpireBatch || time.Now().After(deadline) {
				return storage.ErrStop
			}
			return nil
		})
	if err != nil && !storage.IsStop(err) {
		return 0, err
	}
	if collected == 0 {
		return 0, nil
	}
	if err := x.eng.Apply(batch); err != nil {
		return 0, err
	}
	x.expired.Add(int64(collected))
	return collected, nil
}

// DeleteNow removes a key's persisted state immediately. Used when a command
// discovers an expired key outside of the cycle.
func (x *Expirer) DeleteNow(db uint16, key string, expireAt int64) error {
	batch := x.eng.Batch()
	defer batch.Close()

	if err := batch.Delete(storage.EncodeDataKey(db, key)); err != nil {
		return err
	}
	if expireAt != 0 {
		if err := batch.Delete(storage.EncodeExpireKey(expireAt, db, key)); err != nil {
			return err
		}
	}
	return x.eng.Apply(batch)
}
