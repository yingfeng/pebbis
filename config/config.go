// Package config holds the tunable knobs of a redistore instance.
package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// EvictionPolicy selects which keys are candidates when maxmemory is reached.
type EvictionPolicy string

const (
	NoEviction     EvictionPolicy = "noeviction"
	AllKeysLRU     EvictionPolicy = "allkeys-lru"
	AllKeysRandom  EvictionPolicy = "allkeys-random"
	VolatileLRU    EvictionPolicy = "volatile-lru"
	VolatileRandom EvictionPolicy = "volatile-random"
	VolatileTTL    EvictionPolicy = "volatile-ttl"
	AllKeysLFU     EvictionPolicy = "allkeys-lfu"
	VolatileLFU    EvictionPolicy = "volatile-lfu"
)

// EvictionMode decides what happens to the persisted copy when a key is evicted.
type EvictionMode string

const (
	// EvictPersist only drops the in-memory entry. The value stays in Pebble and
	// is reloaded on the next access. Memory is a hard cap, data is never lost.
	EvictPersist EvictionMode = "persist"
	// EvictCache deletes the Pebble copy too. Pure cache semantics.
	EvictCache EvictionMode = "cache"
)

// LoadMode decides how the in-memory dict is rebuilt at startup.
type LoadMode string

const (
	// LoadAll scans Pebble and rebuilds the whole dict before serving.
	LoadAll LoadMode = "load-all"
	// LoadLazy starts immediately and fills the dict on demand.
	LoadLazy LoadMode = "lazy"
)

// SyncPolicy maps onto Redis' appendfsync.
type SyncPolicy string

const (
	// SyncGroup commits writes without fsync and lets Pebble batch WAL syncs.
	// A crash may lose the last WALMinSyncInterval worth of writes.
	SyncGroup SyncPolicy = "group"
	// SyncAlways fsyncs every write.
	SyncAlways SyncPolicy = "always"
	// SyncNever relies on the OS to flush. Fastest, weakest guarantee.
	SyncNever SyncPolicy = "never"
)

// Object type tags stored in the value header.
const (
	TypeString byte = 0
	TypeHash   byte = 1
	TypeList   byte = 2
	TypeSet    byte = 3
	TypeZSet   byte = 4
)

// TypeName returns the Redis type name for a type tag.
func TypeName(t byte) string {
	switch t {
	case TypeString:
		return "string"
	case TypeHash:
		return "hash"
	case TypeList:
		return "list"
	case TypeSet:
		return "set"
	case TypeZSet:
		return "zset"
	}
	return "none"
}

// Config is the full configuration of a Store.
type Config struct {
	// Dir is the Pebble data directory. Empty means pure in-memory (a Pebble
	// instance backed by a memory filesystem) - useful for tests and caches.
	Dir string
	// RequirePass, when set, makes connections authenticate with AUTH before
	// any other command is accepted.
	RequirePass string

	// MaxMemory is the budget for the in-memory dict, in bytes. Zero disables
	// eviction entirely (the dict then grows without bound).
	MaxMemory uint64
	// EvictionPolicy is the strategy used once MaxMemory is exceeded.
	EvictionPolicy EvictionPolicy
	// EvictionMode decides whether an eviction also deletes the Pebble copy.
	EvictionMode EvictionMode
	// EvictionSample is how many random keys are sampled per eviction round.
	// Redis' default of 5 is a good trade-off between accuracy and cost.
	EvictionSample int
	// LFUDecayMinutes is the LFU counter's half-life period: a key idle for
	// this many minutes loses one point. Redis' default is 1.
	LFUDecayMinutes int
	// EvictionTargetRatio is the fraction of MaxMemory to evict down to once
	// the limit is crossed. Evicting to exactly MaxMemory would re-trigger on
	// the very next write, so we aim a bit lower.
	EvictionTargetRatio float64

	// LuaTimeLimit mirrors Redis' lua-time-limit: seconds after which a busy
	// script is considered stuck. It is accepted and reported by CONFIG but
	// only advisory here.
	LuaTimeLimit int

	// Databases is the number of addressable logical databases (SELECT n).
	Databases int
	// ShardCount is the number of dict shards per database. Power of two.
	ShardCount int
	// InlineValueMaxSize is the largest value kept inline in the dict, avoiding
	// a Pebble read on the hot path.
	InlineValueMaxSize int

	// LoadMode controls startup dict rebuild behaviour.
	LoadMode LoadMode

	// BlockCacheSize is the Pebble block cache budget in bytes. This is where
	// the actual value bytes live, and the reason memory stays bounded.
	BlockCacheSize int64
	// MemTableSize per Pebble memtable.
	MemTableSize uint64
	// L0CompactionThreshold / L0StopWritesThreshold tune write stall behaviour.
	L0CompactionThreshold int
	L0StopWritesThreshold int
	// SyncPolicy controls durability of writes.
	SyncPolicy SyncPolicy
	// WALMinSyncInterval is the group-commit window used by SyncGroup.
	WALMinSyncInterval time.Duration

	// ExpireCycleInterval is how often the active expiry cycle runs.
	ExpireCycleInterval time.Duration
	// ExpireCycleBudget caps a single active expiry round.
	ExpireCycleBudget time.Duration
	// ExpireBatch is the max number of keys deleted per expiry round.
	ExpireBatch int
}

// DefaultConfig returns a configuration suitable for a single-node cache.
func DefaultConfig() *Config {
	return &Config{
		Dir:                   "",
		MaxMemory:             0,
		EvictionPolicy:        NoEviction,
		EvictionMode:          EvictPersist,
		EvictionSample:        5,
		LFUDecayMinutes:       1,
		EvictionTargetRatio:   0.95,
		Databases:             16,
		ShardCount:            1024,
		InlineValueMaxSize:    256,
		LoadMode:              LoadAll,
		BlockCacheSize:        64 << 20, // 64 MiB
		MemTableSize:          64 << 20,
		L0CompactionThreshold: 8,
		L0StopWritesThreshold: 24,
		SyncPolicy:            SyncGroup,
		WALMinSyncInterval:    200 * time.Microsecond,
		ExpireCycleInterval:   100 * time.Millisecond,
		ExpireCycleBudget:     time.Millisecond,
		ExpireBatch:           256,
	}
}

// Validate normalises the configuration and rejects impossible combinations.
func (c *Config) Validate() error {
	if c.ShardCount <= 0 || (c.ShardCount&(c.ShardCount-1)) != 0 {
		return fmt.Errorf("ShardCount must be a power of two, got %d", c.ShardCount)
	}
	if c.EvictionTargetRatio <= 0 || c.EvictionTargetRatio >= 1 {
		return fmt.Errorf("EvictionTargetRatio must be in (0,1), got %v", c.EvictionTargetRatio)
	}
	if c.EvictionSample <= 0 {
		c.EvictionSample = 5
	}
	if c.InlineValueMaxSize < 0 {
		c.InlineValueMaxSize = 0
	}
	if c.BlockCacheSize <= 0 {
		c.BlockCacheSize = 8 << 20
	}
	if c.MemTableSize == 0 {
		c.MemTableSize = 64 << 20
	}
	if c.L0CompactionThreshold == 0 {
		c.L0CompactionThreshold = 8
	}
	if c.L0StopWritesThreshold == 0 {
		c.L0StopWritesThreshold = 24
	}
	if c.WALMinSyncInterval <= 0 {
		c.WALMinSyncInterval = 200 * time.Microsecond
	}
	if c.ExpireCycleInterval <= 0 {
		c.ExpireCycleInterval = 100 * time.Millisecond
	}
	if c.ExpireCycleBudget <= 0 {
		c.ExpireCycleBudget = time.Millisecond
	}
	if c.ExpireBatch <= 0 {
		c.ExpireBatch = 256
	}
	switch strings.ToLower(string(c.EvictionPolicy)) {
	case string(NoEviction), string(AllKeysLRU), string(AllKeysRandom),
		string(VolatileLRU), string(VolatileRandom), string(VolatileTTL),
		string(AllKeysLFU), string(VolatileLFU):
		c.EvictionPolicy = EvictionPolicy(strings.ToLower(string(c.EvictionPolicy)))
	default:
		return fmt.Errorf("unknown eviction policy %q", c.EvictionPolicy)
	}
	switch strings.ToLower(string(c.EvictionMode)) {
	case string(EvictPersist):
		c.EvictionMode = EvictPersist
	case string(EvictCache):
		c.EvictionMode = EvictCache
	default:
		return fmt.Errorf("unknown eviction mode %q", c.EvictionMode)
	}
	switch strings.ToLower(string(c.LoadMode)) {
	case string(LoadAll):
		c.LoadMode = LoadAll
	case string(LoadLazy):
		c.LoadMode = LoadLazy
	default:
		return fmt.Errorf("unknown load mode %q", c.LoadMode)
	}
	switch strings.ToLower(string(c.SyncPolicy)) {
	case string(SyncGroup):
		c.SyncPolicy = SyncGroup
	case string(SyncAlways):
		c.SyncPolicy = SyncAlways
	case string(SyncNever):
		c.SyncPolicy = SyncNever
	default:
		return fmt.Errorf("unknown sync policy %q", c.SyncPolicy)
	}
	// LRU/LFU metadata is only maintained when a policy needs it and memory is
	// actually bounded; see memory.Dict for how the two interact.
	if c.MaxMemory == 0 && c.EvictionPolicy != NoEviction {
		return errors.New("EvictionPolicy requires a non-zero MaxMemory")
	}
	return nil
}

// EvictionEnabled reports whether the store should ever evict.
func (c *Config) EvictionEnabled() bool {
	return c.MaxMemory > 0 && c.EvictionPolicy != NoEviction
}

// TracksLRU reports whether the policy needs last-access timestamps.
func (c *Config) TracksLRU() bool {
	switch c.EvictionPolicy {
	case AllKeysLRU, VolatileLRU, VolatileTTL:
		return true
	}
	return false
}

// TracksLFU reports whether the policy needs access frequency counters.
func (c *Config) TracksLFU() bool {
	switch c.EvictionPolicy {
	case AllKeysLFU, VolatileLFU:
		return true
	}
	return false
}

// VolatileOnly reports whether the policy only considers keys with a TTL.
func (c *Config) VolatileOnly() bool {
	switch c.EvictionPolicy {
	case VolatileLRU, VolatileRandom, VolatileTTL, VolatileLFU:
		return true
	}
	return false
}
