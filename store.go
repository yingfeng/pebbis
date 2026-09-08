// Package redistore is an embeddable, Redis-protocol-compatible store with
// Pebble-backed persistence and bounded memory.
//
// It can be used as a Go library with zero network overhead:
//
//	s, err := redistore.Open(redistore.DefaultOptions())
//	defer s.Close()
//	_ = s.Set(ctx, 0, "k", []byte("v"))
//
// or as a standalone RESP server:
//
//	srv := redistore.NewServer(s, redistore.ServerOptions{Addr: ":6379"})
//	log.Fatal(srv.ListenAndServe())
package redistore

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/memory"
	"github.com/redistore/redistore/storage"
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
	cfg  *config.Config
	eng  *storage.Engine
	dict *memory.Dict
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

	// cfgMu guards mutations of cfg made by CONFIG SET. Readers tolerate a
	// slightly stale view: every knob it protects is a heuristic (eviction
	// thresholds, sample counts), never a correctness constraint.
	cfgMu   sync.RWMutex
	slowLog *SlowLog

	// shutdown is closed by SHUTDOWN; the embedding process decides what to do.
	shutdown chan struct{}

	// scanCursors backs SCAN: the wire cursor must be a number (every client
	// parses it as uint64) while the server needs the last key handed out to
	// resume in O(1), so the mapping is kept here.
	scanCursors *scanCursorTable

	stats Stats
}

// memoryFull reports whether write commands must be rejected with an OOM
// error: a maxmemory budget is configured, the policy is noeviction, and
// usage has already reached the budget. Matches Redis' noeviction behaviour;
// with an eviction policy the store evicts instead of refusing.
func (s *Store) memoryFull() bool {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg.MaxMemory > 0 && s.cfg.EvictionPolicy == config.NoEviction &&
		uint64(s.dict.Used()) >= s.cfg.MaxMemory
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
	dict := memory.NewDict(cfg, clock)

	s := &Store{
		cfg:      cfg,
		eng:      eng,
		dict:     dict,
		clock:    clock,
		stop:        make(chan struct{}),
		shutdown:    make(chan struct{}),
		slowLog:     newSlowLog(),
		scanCursors: newScanCursorTable(),
		stats:       Stats{startTime: time.Now()},
	}

	// Eviction in cache mode has to remove the persisted copy too.
	var del func(db uint16, key string) error
	if cfg.EvictionMode == config.EvictCache {
		del = func(db uint16, key string) error { return s.deletePersisted(db, key) }
	}
	s.evictor = memory.NewEvictor(cfg, dict, del)
	s.expirer = memory.NewExpirer(eng, dict, clock, cfg)
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

	for db := range s.cfg.Databases {
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
func (s *Store) Config() *config.Config { return s.cfg }

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
var ErrClosed = errors.New("redistore: store is closed")

// checkAlive guards command execution after Close.
func (s *Store) checkAlive() error {
	if s.closed.Load() {
		return ErrClosed
	}
	return nil
}
