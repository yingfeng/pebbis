package storage

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/redistore/redistore/config"
)

// ErrNotFound mirrors pebble.ErrNotFound so callers need not import pebble.
var ErrNotFound = pebble.ErrNotFound

// Engine owns a Pebble instance and the block cache that backs it.
//
// The block cache is the real "memory" of the store: value bytes are served from
// it, not from the Go heap. That is what makes MaxMemory a hard limit rather
// than a GC hint.
type Engine struct {
	dir      string
	db       *pebble.DB
	cache    *pebble.Cache
	writeOpt *pebble.WriteOptions
	inMemory bool
}

// Open creates or reopens a Pebble store described by cfg.
// An empty cfg.Dir yields a memory-backed store.
func Open(cfg *config.Config) (*Engine, error) {
	opts := &pebble.Options{
		Cache:                       pebble.NewCache(cfg.BlockCacheSize),
		MemTableSize:                cfg.MemTableSize,
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       cfg.L0CompactionThreshold,
		L0StopWritesThreshold:       cfg.L0StopWritesThreshold,
		MaxConcurrentCompactions:    func() int { return min(8, max(2, runtime.NumCPU()/2)) },
		Levels:                      levelOptions(),
	}

	inMemory := cfg.Dir == ""
	if inMemory {
		opts.FS = vfs.NewMem()
		opts.DisableWAL = true
		cfg.Dir = "" // pebble requires an empty dir when using an in-memory FS
	} else {
		opts.FS = vfs.Default
	}
	opts.WALMinSyncInterval = func() time.Duration { return cfg.WALMinSyncInterval }
	opts.EnsureDefaults()

	db, err := pebble.Open(cfg.Dir, opts)
	if err != nil {
		opts.Cache.Unref()
		return nil, fmt.Errorf("open pebble at %q: %w", cfg.Dir, err)
	}

	var wo *pebble.WriteOptions
	switch cfg.SyncPolicy {
	case config.SyncAlways:
		wo = pebble.Sync
	case config.SyncNever, config.SyncGroup:
		// SyncGroup relies on WALMinSyncInterval to batch fsyncs; the write
		// itself is still appended to the WAL synchronously.
		wo = pebble.NoSync
	}
	if inMemory {
		wo = pebble.NoSync
	}

	return &Engine{
		dir:      cfg.Dir,
		db:       db,
		cache:    opts.Cache,
		writeOpt: wo,
		inMemory: inMemory,
	}, nil
}

// levelOptions configures block size, compression and bloom filters per level.
// Bloom filters matter a lot here: the dict misses go straight to Pebble, and
// most of them are for keys that were never written.
func levelOptions() []pebble.LevelOptions {
	levels := make([]pebble.LevelOptions, 7)
	for i := range levels {
		l := &levels[i]
		l.BlockSize = 32 << 10
		l.IndexBlockSize = 256 << 10
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		l.Compression = pebble.SnappyCompression
	}
	return levels
}

// Get returns the value stored at key. The returned slice is only valid until
// release is called; callers that need to keep it must copy.
func (e *Engine) Get(key []byte) (value []byte, release func(), err error) {
	v, closer, err := e.db.Get(key)
	if err != nil {
		return nil, nil, err
	}
	return v, func() { _ = closer.Close() }, nil
}

// GetCopy returns a heap copy of the value stored at key.
func (e *Engine) GetCopy(key []byte) ([]byte, error) {
	v, closer, err := e.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

// Put writes a single key.
func (e *Engine) Put(key, value []byte) error {
	return e.db.Set(key, value, e.writeOpt)
}

// Delete removes a single key.
func (e *Engine) Delete(key []byte) error {
	return e.db.Delete(key, e.writeOpt)
}

// Batch returns a fresh write batch. The caller must Close it.
func (e *Engine) Batch() *Batch {
	return &Batch{b: e.db.NewBatch()}
}

// Apply commits a batch atomically.
func (e *Engine) Apply(b *Batch) error {
	return e.db.Apply(b.b, e.writeOpt)
}

// DeleteRange removes every key in [start, end).
func (e *Engine) DeleteRange(start, end []byte) error {
	return e.db.DeleteRange(start, end, e.writeOpt)
}

// Scan visits every key/value under prefix in lexicographic order. It stops
// early if fn returns an error. The value slice is only valid during the call.
func (e *Engine) Scan(prefix []byte, fn func(key, value []byte) error) error {
	return e.ScanRange(prefix, prefixEnd(prefix), fn)
}

// ScanRange visits every key/value in [start, end).
func (e *Engine) ScanRange(start, end []byte, fn func(key, value []byte) error) error {
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for valid := iter.First(); valid; valid = iter.Next() {
		if err := fn(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}

// LastInRange returns the greatest key/value in [start, end] (both inclusive)
// via a reverse seek. This is what makes XADD's ID generation O(logN) instead
// of a scan of the whole stream: "the newest entry" is one SeekLT away.
// The returned key and value are copies; release is nil-safe.
func (e *Engine) LastInRange(start, end []byte) (key, value []byte, found bool, err error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: prefixEnd(end)})
	if err != nil {
		return nil, nil, false, err
	}
	defer func() { _ = iter.Close() }()

	if valid := iter.Last(); valid {
		key = append([]byte(nil), iter.Key()...)
		value = append([]byte(nil), iter.Value()...)
		found = true
	}
	return key, value, found, iter.Error()
}

// ScanKeys visits only the keys under prefix, avoiding value materialisation.
func (e *Engine) ScanKeys(prefix []byte, fn func(key []byte) error) error {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixEnd(prefix),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for valid := iter.First(); valid; valid = iter.Next() {
		if err := fn(iter.Key()); err != nil {
			return err
		}
	}
	return iter.Error()
}

// Checkpoint creates a consistent hard-link snapshot of the store in destDir.
func (e *Engine) Checkpoint(destDir string) error {
	return e.db.Checkpoint(destDir)
}

// Flush forces memtables to stable storage.
func (e *Engine) Flush() error {
	return e.db.Flush()
}

// Metrics exposes Pebble's internal counters for the INFO command.
func (e *Engine) Metrics() *pebble.Metrics {
	return e.db.Metrics()
}

// BlockCacheBytes returns the current block cache usage.
func (e *Engine) BlockCacheBytes() int64 {
	if e.cache == nil {
		return 0
	}
	return e.cache.Size()
}

// DiskUsageBytes returns an estimate of the on-disk footprint.
func (e *Engine) DiskUsageBytes() uint64 {
	m := e.db.Metrics()
	var total uint64
	for _, lm := range m.Levels {
		total += uint64(lm.Size)
	}
	total += uint64(m.WAL.Size)
	return total
}

// Close releases the store. It must not be called concurrently with other
// Engine methods.
func (e *Engine) Close() error {
	err := e.db.Close()
	if e.cache != nil {
		e.cache.Unref()
		e.cache = nil
	}
	return err
}

// Batch is a thin wrapper that keeps callers from importing pebble directly.
type Batch struct {
	b *pebble.Batch
}

// Set stages a write.
func (b *Batch) Set(key, value []byte) error {
	return b.b.Set(key, value, nil)
}

// Delete stages a deletion.
func (b *Batch) Delete(key []byte) error {
	return b.b.Delete(key, nil)
}

// DeleteRange stages a range deletion.
func (b *Batch) DeleteRange(start, end []byte) error {
	return b.b.DeleteRange(start, end, nil)
}

// Empty reports whether nothing has been staged yet.
func (b *Batch) Empty() bool {
	return b.b.Empty()
}

// Reset clears the batch so it can be reused.
func (b *Batch) Reset() {
	b.b.Reset()
}

// Close releases the batch.
func (b *Batch) Close() error {
	return b.b.Close()
}

// prefixEnd returns the lexicographic successor of a prefix, i.e. the exclusive
// upper bound that covers exactly the keys carrying that prefix.
func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // prefix is all 0xff; no upper bound
}

// ErrStop is a sentinel that Scan callers may return to stop early without
// reporting a failure.
var ErrStop = errors.New("redistore: stop iteration")

// IsStop reports whether err is ErrStop.
func IsStop(err error) bool {
	return errors.Is(err, ErrStop)
}
