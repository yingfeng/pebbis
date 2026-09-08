// Package Pebbis is an embeddable, Redis-protocol-compatible store with
// Pebble-backed persistence and bounded memory.
//
// It can be used as a Go library with zero network overhead:
//
//	s, err := pebbis.Open(pebbis.DefaultOptions())
//	defer s.Close()
//	_ = s.Set(ctx, 0, "k", []byte("v"))
//
// or as a standalone RESP server:
//
//	srv := pebbis.NewServer(s, pebbis.ServerOptions{Addr: ":6379"})
//	log.Fatal(srv.ListenAndServe())
package pebbis

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pebbis/pebbis/config"
	"github.com/pebbis/pebbis/memory"
	"github.com/pebbis/pebbis/storage"
)

// Options configures a Store. It aliases config.Config so callers can use
// DefaultOptions() and then tweak fields.
type Options = config.Config

// DefaultOptions returns a configuration suitable for a single-node cache.
func DefaultOptions() *Options { return config.DefaultConfig() }

// Stats holds the counters surfaced by INFO.
type Stats struct {
	hits       atomic.Int64
	misses     atomic.Int64
	commands   atomic.Int64
	expired    atomic.Int64
	startTime  time.Time
	keyChanges atomic.Int64
}

// Store is a single logical instance: one Pebble store plus its in-memory index.
//
// Nothing here depends on the network layer, which is what makes the embedded
// use case cheap - see api.go for the direct-call surface.
type Store struct {
	cfg  atomic.Pointer[config.Config]
	eng  *storage.Engine
	dict *memory.Dict
	// keyMu is a sharded reader/writer lock indexed by (db, key). A command that
	// touches a key takes the exclusive lock for the whole command; a read that
	// scans an aggregate takes the shared lock. Different keys (almost always)
	// map to different shards, so independent keys are served concurrently on
	// multiple cores instead of being funnelled through one global mutex - this
	// is the correct granularity (Redis' atomicity guarantee is per-key, not
	// global) and lets Pebble's own internal concurrency do the heavy lifting.
	keyMu keyLockTable
	// aggMu is a SECOND, independent per-key sharded lock taken inside the
	// aggregate storage layer (scanHash/scanSet/scanZSet/scanList vs saveAgg and
	// the list mutations). It is separate from keyMu on purpose: dispatch holds
	// keyMu for the whole command, so reusing it here would deadlock (Go's
	// sync.RWMutex is not reentrant). Like keyMu it is key-sharded, so two
	// commands on different keys no longer serialise against each other the way
	// the old single global aggregate mutex did.
	aggMu keyLockTable
	// clock is exported to subpackages via accessor; kept unexported to keep
	// the API surface small.
	clock *memory.Clock

	evictor *memory.Evictor
	expirer *memory.Expirer
	// blocker wakes commands blocked on BLPOP/BRPOP.
	blocker *blocker

	stop   chan struct{}
	wg     sync.WaitGroup
	closed atomic.Bool

	lastSave atomic.Int64

	slowLog *SlowLog

	// shutdown is closed by SHUTDOWN; the embedding process decides what to do.
	shutdown chan struct{}

	// scanCursors backs SCAN: the wire cursor must be a number (every client
	// parses it as uint64) while the server needs the last key handed out to
	// resume in O(1), so the mapping is kept here.
	scanCursors *scanCursorTable

	stats Stats
}

// keyLockTable is a fixed array of reader/writer mutexes sharded by (db, key).
// This mirrors kvrocks' LockManager: a single command takes the lock for the
// shard(s) its key(s) hash to, so writers to different keys land on different
// shards and run in parallel (Pebble's own internal concurrency is finally
// used) while writers to the SAME key are serialised, giving Redis' per-key
// atomicity without funnelling every command through one global mutex. A fixed
// array (not a slice) means the zero value is ready to use, so no initialisation
// is needed.
type keyLockTable struct {
	shards [keyLockShards]sync.RWMutex
}

// keyLockShards is a power of two so shardIndex can mask instead of divide.
// 8192 shards matches kvrocks' hash_power of 16 (65536) within an order of
// magnitude while keeping the embedded-memory footprint small; raise it if
// contention on hot keys shows up in profiles.
const keyLockShards = 1 << 13 // 8192 shards

func (t *keyLockTable) lock(db uint16, key string)   { t.shards[shardIndex(db, key)].Lock() }
func (t *keyLockTable) unlock(db uint16, key string) { t.shards[shardIndex(db, key)].Unlock() }
func (t *keyLockTable) rlock(db uint16, key string)   { t.shards[shardIndex(db, key)].RLock() }
func (t *keyLockTable) runlock(db uint16, key string) { t.shards[shardIndex(db, key)].RUnlock() }

// LockKeys acquires the per-key locks for every key in keys and returns an
// unlock function. It mirrors kvrocks' MultiLockGuard: the shard indexes are
// de-duplicated and then locked in a canonical DESCENDING order so that two
// commands locking overlapping but differently-ordered key sets (e.g.
// "RENAME a b" vs "RENAME b a") can never deadlock. The returned closure
// releases the locks in the reverse (ascending) order.
//
// The lock is exclusive (write) or shared (read) for all keys uniformly: a write
// command takes an exclusive lock on every key it touches (read or written),
// exactly as kvrocks locks all of a command's keys for a kCmdWrite command.
func (t *keyLockTable) lockAll(db uint16, keys []string, write bool) func() {
	if len(keys) == 0 {
		return func() {}
	}
	idxs := make([]int, 0, len(keys))
	seen := make(map[int]struct{}, len(keys))
	for _, k := range keys {
		i := shardIndex(db, k)
		if _, ok := seen[i]; ok {
			continue // same shard already in the set: lock it once
		}
		seen[i] = struct{}{}
		idxs = append(idxs, i)
	}
	// Canonical order: descending shard index. kvrocks uses
	// std::set<unsigned, std::greater<unsigned>> for exactly this reason.
	sort.Sort(sort.Reverse(sort.IntSlice(idxs)))
	for _, i := range idxs {
		if write {
			t.shards[i].Lock()
		} else {
			t.shards[i].RLock()
		}
	}
	return func() {
		for i := len(idxs) - 1; i >= 0; i-- {
			if write {
				t.shards[idxs[i]].Unlock()
			} else {
				t.shards[idxs[i]].RUnlock()
			}
		}
	}
}

// LockKey/UnlockKey/RLockKey/RUnlockKey are the single-key entry points used by
// callers that already know the (db, key) they touch for the whole operation
// (the embedded API, blocking commands that re-lock inside their handler).
func (s *Store) LockKey(db uint16, key string)   { s.keyMu.lock(db, key) }
func (s *Store) UnlockKey(db uint16, key string) { s.keyMu.unlock(db, key) }
func (s *Store) RLockKey(db uint16, key string)   { s.keyMu.rlock(db, key) }
func (s *Store) RUnlockKey(db uint16, key string) { s.keyMu.runlock(db, key) }

// LockKeys is the multi-key entry point used by command dispatch. See
// keyLockTable.lockAll for the ordering and deadlock-avoidance contract.
func (s *Store) LockKeys(db uint16, keys []string, write bool) func() {
	return s.keyMu.lockAll(db, keys, write)
}

// shardIndex maps (db, key) to a shard with FNV-1a. Cheap and good enough to
// spread distinct keys across shards; collisions only cost extra (correct)
// serialisation, never incorrectness.
func shardIndex(db uint16, key string) int {
	h := uint64(1469598103934665603) // FNV offset basis
	for _, c := range [2]byte{byte(db >> 8), byte(db)} {
		h ^= uint64(c)
		h *= 16777619
	}
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 16777619
	}
	return int(h & (keyLockShards - 1))
}

// memoryFull reports whether write commands must be rejected with an OOM
// error: a maxmemory budget is configured, the policy is noeviction, and
// usage has already reached the budget. Matches Redis' noeviction behaviour;
// with an eviction policy the store evicts instead of refusing.
func (s *Store) memoryFull() bool {
	c := s.cfg.Load()
	return c.MaxMemory > 0 && c.EvictionPolicy == config.NoEviction &&
		uint64(s.dict.Used()) >= c.MaxMemory
}

// Shutdown returns a channel that is closed when a client issues SHUTDOWN.
// A standalone server uses it to exit; an embedded caller may ignore it.
func (s *Store) Shutdown() <-chan struct{} { return s.shutdown }

func (s *Store) requestShutdown() {
	select {
	case <-s.shutdown:
	default:
		close(s.shutdown)
	}
}

// Open creates or reopens a store. An empty cfg.Dir yields a memory-backed
// store, which is handy for tests and for pure-cache deployments.
func Open(cfg *config.Config) (*Store, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	eng, err := storage.Open(cfg)
	if err != nil {
		return nil, err
	}

	clock := memory.NewClock()
	s := &Store{
		eng:      eng,
		clock:    clock,
		stop:        make(chan struct{}),
		shutdown:    make(chan struct{}),
		slowLog:     newSlowLog(),
		scanCursors: newScanCursorTable(),
		stats:       Stats{startTime: time.Now()},
	}
	s.cfg.Store(cfg)

	dict := memory.NewDict(&s.cfg, clock)
	s.dict = dict

	// Eviction in cache mode has to remove the persisted copy too.
	var del func(db uint16, key string) error
	if cfg.EvictionMode == config.EvictCache {
		del = func(db uint16, key string) error { return s.deletePersisted(db, key) }
	}
	s.evictor = memory.NewEvictor(&s.cfg, dict, del)
	s.expirer = memory.NewExpirer(eng, dict, clock, &s.cfg)
	s.blocker = newBlocker()

	if cfg.LoadMode == config.LoadAll {
		if err := s.loadAll(); err != nil {
			_ = eng.Close()
			clock.Stop()
			return nil, err
		}
	}

	s.wg.Add(2)
	go func() { defer s.wg.Done(); s.expirer.Run(s.stop) }()
	go func() { defer s.wg.Done(); s.evictor.Run(s.stop) }()

	return s, nil
}

// loadAll rebuilds the in-memory index by scanning Pebble, dropping keys whose
// TTL has already passed.
func (s *Store) loadAll() error {
	now := s.clock.NowMilli()
	batch := s.eng.Batch()
	defer batch.Close()
	pending := 0

	for db := range s.cfg.Load().Databases {
		u16 := uint16(db)
		err := s.eng.Scan(storage.DataPrefix(u16), func(k, v []byte) error {
			typ, expireAt, payload, err := storage.DecodeValue(v)
			if err != nil {
				return nil // skip corrupt records rather than fail startup
			}
			key := storage.KeyFromDataKey(k)
			if expireAt != 0 && expireAt <= now {
				// Drop the value and its expiry index entry.
				_ = batch.Delete(storage.EncodeDataKey(u16, key))
				_ = batch.Delete(storage.EncodeExpireKey(expireAt, u16, key))
				pending++
				return nil
			}
			s.dict.Set(u16, key, typ, expireAt, payload)
			return nil
		})
		if err != nil {
			return fmt.Errorf("scan db %d: %w", db, err)
		}
	}

	if pending > 0 {
		if err := s.eng.Apply(batch); err != nil {
			return err
		}
		s.stats.expired.Add(int64(pending))
	}
	return nil
}

// Close stops background workers and releases the store. It is safe to call
// more than once.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(s.stop)
	s.wg.Wait()
	s.clock.Stop()
	return s.eng.Close()
}

// Config returns the live configuration.
func (s *Store) Config() *config.Config { return s.cfg.Load() }

// DBCount returns the number of addressable logical databases.
func (s *Store) DBCount() int { return s.dict.Databases() }

// Engine exposes the Pebble wrapper, for backup and metrics.
func (s *Store) Engine() *storage.Engine { return s.eng }

// Checkpoint writes a consistent hard-link snapshot to destDir.
func (s *Store) Checkpoint(destDir string) error { return s.eng.Checkpoint(destDir) }

// Flush forces memtables to stable storage.
func (s *Store) Flush() error { return s.eng.Flush() }

// UsedMemory returns the estimated bytes held by the in-memory index.
func (s *Store) UsedMemory() int64 { return s.dict.Used() }

// BlockCacheMemory returns the current Pebble block cache usage.
func (s *Store) BlockCacheMemory() int64 { return s.eng.BlockCacheBytes() }

// DiskUsage returns an estimate of the on-disk footprint.
func (s *Store) DiskUsage() uint64 { return s.eng.DiskUsageBytes() }

// Uptime returns how long the store has been running.
func (s *Store) Uptime() time.Duration { return time.Since(s.stats.startTime) }

// ErrClosed is returned once the store has been closed.
var ErrClosed = errors.New("Pebbis: store is closed")

// checkAlive guards command execution after Close.
func (s *Store) checkAlive() error {
	if s.closed.Load() {
		return ErrClosed
	}
	return nil
}
