package memory

import (
	"hash/maphash"
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"github.com/redistore/redistore/config"
)

var dictSeed = maphash.MakeSeed()

// shard is one independently locked partition of a logical database.
type shard struct {
	idx int // position in dbShards.shards, used to order multi-shard locks
	mu  sync.RWMutex
	m   map[string]*Entry
}

// dbShards is the full shard set of a single logical database.
//
// It also tracks which shards currently hold keys. That list is what makes
// eviction sampling work when the keyspace is sparse: with 1024 shards and a
// few hundred keys, picking a shard at random finds an empty one almost every
// time, and eviction would stall.
type dbShards struct {
	shards []*shard
	mask   uint64

	mu       sync.RWMutex
	nonEmpty []int       // shard indices holding at least one entry
	pos      map[int]int // shard index -> slot in nonEmpty, for O(1) removal
}

func newDBShards(n int) *dbShards {
	shards := make([]*shard, n)
	for i := range shards {
		shards[i] = &shard{idx: i, m: make(map[string]*Entry)}
	}
	return &dbShards{
		shards: shards,
		mask:   uint64(n - 1),
		pos:    make(map[int]int),
	}
}

func (d *dbShards) shardFor(key string) *shard {
	return d.shards[maphash.String(dictSeed, key)&d.mask]
}

// trackInsert records that a shard became non-empty. Called with the shard
// unlocked; it acquires dbShards.mu, so callers must not hold it.
func (d *dbShards) trackInsert(s *shard) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.pos[s.idx]; ok {
		return
	}
	d.pos[s.idx] = len(d.nonEmpty)
	d.nonEmpty = append(d.nonEmpty, s.idx)
}

// trackEmpty records that a shard became empty.
func (d *dbShards) trackEmpty(s *shard) {
	d.mu.Lock()
	defer d.mu.Unlock()
	p, ok := d.pos[s.idx]
	if !ok {
		return
	}
	last := len(d.nonEmpty) - 1
	d.nonEmpty[p] = d.nonEmpty[last]
	d.pos[d.nonEmpty[p]] = p
	d.nonEmpty = d.nonEmpty[:last]
	delete(d.pos, s.idx)
}

// randomShard returns a shard that held keys at the time of the call, or nil
// when the database is empty. dbShards.mu is released before returning, so
// callers may safely take the shard lock afterwards.
func (d *dbShards) randomShard() *shard {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.nonEmpty) == 0 {
		return nil
	}
	return d.shards[d.nonEmpty[rand.IntN(len(d.nonEmpty))]]
}

// Dict is the sharded in-memory index over every logical database.
//
// Only metadata and small inline values live here. That is the whole point: the
// dict scales with the number of keys, not with the volume of data, so
// MaxMemory bounds something we can actually measure.
type Dict struct {
	cfg     *atomic.Pointer[config.Config]
	clock   *Clock
	dbCount int
	dbs     []atomic.Pointer[dbShards]

	used atomic.Int64 // estimated bytes held by the dict
}

// NewDict creates an empty dict of cfg.Databases logical databases.
func NewDict(cfg *atomic.Pointer[config.Config], clock *Clock) *Dict {
	n := cfg.Load().Databases
	if n <= 0 {
		n = 16
	}
	return &Dict{
		cfg:     cfg,
		clock:   clock,
		dbCount: n,
		dbs:     make([]atomic.Pointer[dbShards], n),
	}
}

// Databases returns the number of addressable logical databases.
func (d *Dict) Databases() int { return d.dbCount }

// Valid reports whether db is addressable.
func (d *Dict) Valid(db uint16) bool { return int(db) < d.dbCount }

// dbShards returns (creating if needed) the shard set of a logical database.
func (d *Dict) dbShards(db uint16) *dbShards {
	if p := d.dbs[db].Load(); p != nil {
		return p
	}
	fresh := newDBShards(d.cfg.Load().ShardCount)
	if d.dbs[db].CompareAndSwap(nil, fresh) {
		return fresh
	}
	return d.dbs[db].Load()
}

// Lookup returns the entry for key without touching access metadata. It reports
// whether the entry exists and is not expired; expired entries are dropped.
func (d *Dict) Lookup(db uint16, key string) (*Entry, bool) {
	s := d.dbShards(db).shardFor(key)
	now := d.clock.NowMilli()

	s.mu.RLock()
	e, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if e.Expired(now) {
		d.Delete(db, key)
		return nil, false
	}
	return e, true
}

// SetCounter records that key is a counter maintained by the merge-operator INCR
// path. It stores metadata only (no inline value) and flags the entry so reads
// fall through to Pebble, where the authoritative merged value lives. The entry
// is created if absent, so EXISTS/DBSIZE/TYPE observe the key immediately.
func (d *Dict) SetCounter(db uint16, key string, expireAt int64) {
	ds := d.dbShards(db)
	s := ds.shardFor(key)
	clock := d.clock.LRUClock()
	s.mu.Lock()
	if old, ok := s.m[key]; ok {
		old.SetCounter(true)
		old.SetExpiry(expireAt)
		old.version.Store(old.bumpVersion())
		s.mu.Unlock()
		return
	}
	e := newEntry(key, config.TypeString, expireAt, nil, d.cfg.Load().InlineValueMaxSize, clock)
	e.SetCounter(true)
	e.lru = (uint32(lfuInitVal) << LRUClockBits) | (clock & lruClockMask)
	e.version.Store(1)
	s.m[key] = e
	d.used.Add(int64(e.size))
	s.mu.Unlock()
	ds.trackInsert(s)
}

// Touch records an access for eviction bookkeeping. It is separate from Lookup
// so that read paths can skip it when no eviction policy needs the data.
func (d *Dict) Touch(db uint16, key string) {
	clock := d.clock.LRUClock()
	decay := d.cfg.Load().LFUDecayMinutes
	s := d.dbShards(db).shardFor(key)
	s.mu.RLock()
	if e, ok := s.m[key]; ok {
		// LFU decay is lazy: applied here, on access, rather than by a sweep.
		e.Decay(clock, decay)
		e.Touch(clock)
		e.IncrFreq()
	}
	s.mu.RUnlock()
}

// Set stores or replaces an entry, preserving its access history on overwrite.
func (d *Dict) Set(db uint16, key string, typ uint8, expireAt int64, payload []byte) *Entry {
	ds := d.dbShards(db)
	s := ds.shardFor(key)
	clock := d.clock.LRUClock()

	s.mu.Lock()
	e := newEntry(key, typ, expireAt, payload, d.cfg.Load().InlineValueMaxSize, clock)
	e.lru = (uint32(lfuInitVal) << LRUClockBits) | (clock & lruClockMask)

	_, existed := s.m[key]
	if old, ok := s.m[key]; ok {
		e.lru = old.lru // an overwrite is not an access
		e.version.Store(old.bumpVersion())
		d.used.Add(int64(e.size) - int64(old.size))
	} else {
		e.version.Store(1)
		d.used.Add(int64(e.size))
	}
	s.m[key] = e
	s.mu.Unlock()

	if !existed {
		ds.trackInsert(s)
	}
	return e
}

// SetExpiry updates the expiry of an existing key. The second return value
// reports whether the key was present (and not already expired).
func (d *Dict) SetExpiry(db uint16, key string, expireAt int64) (*Entry, bool) {
	ds := d.dbShards(db)
	s := ds.shardFor(key)
	now := d.clock.NowMilli()

	s.mu.Lock()
	e, ok := s.m[key]
	if !ok {
		s.mu.Unlock()
		return nil, false
	}
	if e.Expired(now) {
		delete(s.m, key)
		empty := len(s.m) == 0
		d.used.Add(-int64(e.size))
		s.mu.Unlock()
		if empty {
			ds.trackEmpty(s)
		}
		return nil, false
	}
	e.SetExpiry(expireAt)
	s.mu.Unlock()
	return e, true
}

// Delete removes an entry and returns it.
func (d *Dict) Delete(db uint16, key string) (*Entry, bool) {
	ds := d.dbShards(db)
	s := ds.shardFor(key)

	s.mu.Lock()
	e, ok := s.m[key]
	if !ok {
		s.mu.Unlock()
		return nil, false
	}
	delete(s.m, key)
	empty := len(s.m) == 0
	d.used.Add(-int64(e.size))
	s.mu.Unlock()

	if empty {
		ds.trackEmpty(s)
	}
	return e, true
}

// DeleteExpired removes key if it exists and is expired, reporting whether it
// was removed.
func (d *Dict) DeleteExpired(db uint16, key string, nowMs int64) bool {
	ds := d.dbShards(db)
	s := ds.shardFor(key)

	s.mu.Lock()
	e, ok := s.m[key]
	if !ok || !e.Expired(nowMs) {
		s.mu.Unlock()
		return false
	}
	delete(s.m, key)
	empty := len(s.m) == 0
	d.used.Add(-int64(e.size))
	s.mu.Unlock()

	if empty {
		ds.trackEmpty(s)
	}
	return true
}

// Rename moves an entry from src to dst, returning the moved entry.
func (d *Dict) Rename(db uint16, src, dst string) (*Entry, bool) {
	if src == dst {
		return d.Lookup(db, src)
	}
	ds := d.dbShards(db)
	srcShard := ds.shardFor(src)
	dstShard := ds.shardFor(dst)

	if srcShard == dstShard {
		srcShard.mu.Lock()
		e, ok := renameWithin(srcShard, src, dst, func(delta int64) { d.used.Add(delta) })
		srcShard.mu.Unlock()
		return e, ok
	}
	// Lock shards in index order to avoid deadlock.
	a, b := srcShard, dstShard
	if a.idx > b.idx {
		a, b = b, a
	}
	a.mu.Lock()
	b.mu.Lock()

	e, ok := srcShard.m[src]
	if !ok {
		b.mu.Unlock()
		a.mu.Unlock()
		return nil, false
	}
	delete(srcShard.m, src)
	if old, existed := dstShard.m[dst]; existed {
		d.used.Add(-int64(old.size))
	}
	// The key length may differ, so the footprint has to be recomputed.
	prevSize := e.size
	e.key = dst
	e.size = entryMem(dst, e.inline)
	d.used.Add(int64(e.size) - int64(prevSize))
	dstShard.m[dst] = e

	srcEmpty := len(srcShard.m) == 0
	b.mu.Unlock()
	a.mu.Unlock()

	if srcEmpty {
		ds.trackEmpty(srcShard)
	} else {
		ds.trackInsert(dstShard)
	}
	return e, true
}

func renameWithin(s *shard, src, dst string, adjust func(delta int64)) (*Entry, bool) {
	e, ok := s.m[src]
	if !ok {
		return nil, false
	}
	delete(s.m, src)
	if old, existed := s.m[dst]; existed {
		adjust(-int64(old.size))
	}
	prevSize := e.size
	e.key = dst
	e.size = entryMem(dst, e.inline)
	adjust(int64(e.size) - int64(prevSize))
	s.m[dst] = e
	return e, true
}

// Len returns the number of live keys in a logical database. Expired keys are
// counted; they are reaped by the expiry cycle or the next access.
func (d *Dict) Len(db uint16) int {
	ds := d.dbs[db].Load()
	if ds == nil {
		return 0
	}
	var n int
	for _, s := range ds.shards {
		s.mu.RLock()
		n += len(s.m)
		s.mu.RUnlock()
	}
	return n
}

// Keys returns every key in a logical database. The result is unordered.
func (d *Dict) Keys(db uint16) []string {
	ds := d.dbs[db].Load()
	if ds == nil {
		return nil
	}
	out := make([]string, 0, d.Len(db))
	for _, s := range ds.shards {
		s.mu.RLock()
		for k := range s.m {
			out = append(out, k)
		}
		s.mu.RUnlock()
	}
	return out
}

// Flush drops everything in a logical database.
func (d *Dict) Flush(db uint16) {
	d.dbs[db].Store(newDBShards(d.cfg.Load().ShardCount))
	d.recomputeUsed()
}

// Used returns the estimated bytes held by the dict.
func (d *Dict) Used() int64 { return d.used.Load() }

// recomputeUsed rebuilds the usage counter from scratch. Only called by Flush,
// which invalidates accounting wholesale.
func (d *Dict) recomputeUsed() {
	var total int64
	for db := range d.dbCount {
		ds := d.dbs[db].Load()
		if ds == nil {
			continue
		}
		for _, s := range ds.shards {
			s.mu.RLock()
			for _, e := range s.m {
				total += int64(e.size)
			}
			s.mu.RUnlock()
		}
	}
	d.used.Store(total)
}

// RandomKey returns an arbitrary key of the database, or false when empty.
// Go randomises map range order, so the first key of a random non-empty shard
// is an unbiased draw.
func (d *Dict) RandomKey(db uint16) (string, bool) {
	ds := d.dbs[db].Load()
	if ds == nil {
		return "", false
	}
	s := ds.randomShard()
	if s == nil {
		return "", false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k := range s.m {
		return k, true
	}
	return "", false
}
