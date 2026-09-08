package goclient

// Port of selected scenarios from Redis 8.6.0's official unit/*.tcl suites,
// via frogdb's redis-regression crate (which ported the same TCL files to
// Rust). Only scenarios for commands Pebbis implements are ported; each
// test cites its upstream test name.

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// --- unit/string.tcl ---

// tcl_set_and_get_an_empty_item
func TestTCLSetAndGetEmptyItem(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	if err := c.Set(ctx, "emptykey", "", 0).Err(); err != nil {
		t.Fatalf("set empty: %v", err)
	}
	got, err := c.Get(ctx, "emptykey").Result()
	if err != nil || got != "" {
		t.Fatalf("get empty: %q %v", got, err)
	}
	if n, err := c.Exists(ctx, "emptykey").Result(); err != nil || n != 1 {
		t.Fatalf("exists: %d %v", n, err)
	}
}

// tcl_setnx_against_not_expired_volatile_key
func TestTCLSetNXAgainstVolatileKey(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	c.Set(ctx, "x", "10", 100*time.Second)
	if ok, err := c.SetNX(ctx, "x", "20", 0).Result(); err != nil || ok {
		t.Fatalf("setnx on volatile key: %v %v", ok, err)
	}
	if got, _ := c.Get(ctx, "x").Result(); got != "10" {
		t.Fatalf("value overwritten: %q", got)
	}
}

// tcl_mset_with_already_existing_same_key_twice / not_existing_keys_same_key_twice
func TestTCLMSetDuplicateKeys(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	// Last value wins for a repeated key.
	if err := c.MSet(ctx, "k", "a", "k", "b").Err(); err != nil {
		t.Fatalf("mset dup: %v", err)
	}
	if got, _ := c.Get(ctx, "k").Result(); got != "b" {
		t.Fatalf("mset dup value: %q", got)
	}
	// MSETNX with the same key twice on all-missing keys succeeds.
	if ok, err := c.MSetNX(ctx, "a", "1", "a", "2").Result(); err != nil || !ok {
		t.Fatalf("msetnx dup: %v %v", ok, err)
	}
	if got, _ := c.Get(ctx, "a").Result(); got != "2" {
		t.Fatalf("msetnx dup value: %q", got)
	}
}

// tcl_setrange_with_out_of_range_offset
func TestTCLSetRangeOutOfRange(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	// Redis caps string offsets at proto-max-bulk-len (512MB).
	if err := c.Do(ctx, "SETRANGE", "k", int64(1)<<30, "x").Err(); err == nil {
		t.Fatal("setrange past 512MB must error")
	}
	// A modest in-range offset zero-fills up to it.
	if n, err := c.SetRange(ctx, "k", 5, "x").Result(); err != nil || n != 6 {
		t.Fatalf("setrange fill: %d %v", n, err)
	}
	if got, _ := c.Get(ctx, "k").Result(); got != "\x00\x00\x00\x00\x00x" {
		t.Fatalf("zero fill: %q", got)
	}
}

// tcl_getrange_against_integer_encoded_value + negative edges
func TestTCLGetRangeEdges(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	c.Set(ctx, "num", "1234", 0)
	if got, err := c.GetRange(ctx, "num", -5, -1).Result(); err != nil || got != "1234" {
		t.Fatalf("getrange -5 -1: %q %v", got, err)
	}
	if got, err := c.GetRange(ctx, "num", 5, 10).Result(); err != nil || got != "" {
		t.Fatalf("getrange past end: %q %v", got, err)
	}
	if got, err := c.GetRange(ctx, "num", 3, 100).Result(); err != nil || got != "4" {
		t.Fatalf("getrange clamp: %q %v", got, err)
	}
}

// tcl_extended_set_can_detect_syntax_errors
func TestTCLSetSyntaxErrors(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	for _, args := range [][]string{
		{"SET", "k", "v", "EX"},               // missing value
		{"SET", "k", "v", "EX", "x"},          // non-integer
		{"SET", "k", "v", "NX", "XX"},         // incompatible
		{"SET", "k", "v", "NONSENSE", "more"}, // unknown option
	} {
		full := append([]string{}, args...)
		if err := c.Do(ctx, toIface(full)...).Err(); err == nil {
			t.Errorf("SET %v must error", args)
		}
	}
}

func toIface(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// --- unit/hash.tcl ---

// tcl_hash_commands_against_wrong_type
func TestTCLHashAgainstWrongType(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	c.Set(ctx, "str", "s", 0)
	for _, op := range []func() error{
		func() error { return c.HGet(ctx, "str", "f").Err() },
		func() error { return c.HSet(ctx, "str", "f", "v").Err() },
		func() error { return c.HDel(ctx, "str", "f").Err() },
		func() error { return c.HLen(ctx, "str").Err() },
		func() error { return c.HKeys(ctx, "str").Err() },
	} {
		if err := op(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
			t.Fatalf("hash on string: want WRONGTYPE, got %v", err)
		}
	}
}

// tcl_hdel_hash_becomes_empty_before_deleting_all_specified_fields +
// tcl_hdel_more_than_a_single_value
func TestTCLHDelEmptiesHash(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	c.HSet(ctx, "h", "f1", "v1", "f2", "v2", "f3", "v3")
	// Removing a field while others remain keeps the key alive.
	if n, err := c.HDel(ctx, "h", "f1", "nope").Result(); err != nil || n != 1 {
		t.Fatalf("hdel partial: %d %v", n, err)
	}
	if n, _ := c.Exists(ctx, "h").Result(); n != 1 {
		t.Fatal("hash removed while fields remain")
	}
	// Removing the last field deletes the key.
	if n, err := c.HDel(ctx, "h", "f2", "f3").Result(); err != nil || n != 2 {
		t.Fatalf("hdel rest: %d %v", n, err)
	}
	if n, _ := c.Exists(ctx, "h").Result(); n != 0 {
		t.Fatal("emptied hash must be removed")
	}
}

// tcl_hincrby_over_32bit_value
func TestTCLHIncrBy64Bit(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	c.HSet(ctx, "h", "big", 1<<31)
	if v, err := c.HIncrBy(ctx, "h", "big", 1<<30).Result(); err != nil || v != int64(1<<31)+int64(1<<30) {
		t.Fatalf("hincrby past 32bit: %d %v", v, err)
	}
}

// tcl_hincrby_fails_against_hash_value_with_spaces_left/right
func TestTCLHIncrBySpaces(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	c.HSet(ctx, "h", "pad", " 10")
	if err := c.HIncrBy(ctx, "h", "pad", 1).Err(); err == nil {
		t.Fatal("left-padded value must not incr")
	}
	c.HSet(ctx, "h", "pad2", "10 ")
	if err := c.HIncrBy(ctx, "h", "pad2", 1).Err(); err == nil {
		t.Fatal("right-padded value must not incr")
	}
}

// --- unit/zset.tcl ---

// tcl_zadd_incr_leading_to_nan_is_error (inf + -inf)
func TestTCLZAddIncrNaN(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	// 1e308 + 1e308 overflows to +inf; incrementing by -inf yields NaN,
	// which Redis rejects outright.
	c.ZAdd(ctx, "z", redis.Z{Score: 1e308, Member: "m"})
	c.ZAddArgsIncr(ctx, "z", redis.ZAddArgs{Members: []redis.Z{{Score: 1e308, Member: "m"}}})
	if _, err := c.ZAddArgsIncr(ctx, "z", redis.ZAddArgs{
		Members: []redis.Z{{Score: math.Inf(-1), Member: "m"}},
	}).Result(); err == nil {
		t.Fatal("NaN score must error")
	}
}

// tcl_zrem_removes_key_after_last_element
func TestTCLZRemRemovesKey(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	c.ZAdd(ctx, "z", redis.Z{Score: 1, Member: "only"})
	if n, err := c.ZRem(ctx, "z", "only").Result(); err != nil || n != 1 {
		t.Fatalf("zrem: %d %v", n, err)
	}
	if n, _ := c.Exists(ctx, "z").Result(); n != 0 {
		t.Fatal("emptied zset must be removed")
	}
}

// --- unit/expire.tcl ---

// tcl_expire_set_timeouts_multiple_times / tcl_persist_returns_0...
func TestTCLExpireMultipleTimes(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	c.Set(ctx, "k", "v", 0)
	for i := 0; i < 3; i++ {
		if err := c.Expire(ctx, "k", 100*time.Second).Err(); err != nil {
			t.Fatalf("expire #%d: %v", i, err)
		}
		if v, err := c.Get(ctx, "k").Result(); err != nil || v != "v" {
			t.Fatalf("expire #%d read: %q %v", i, v, err)
		}
	}
	// PERSIST on a non-existing key returns false (0), not an error.
	if ok, err := c.Persist(ctx, "missing").Result(); err != nil || ok {
		t.Fatalf("persist missing: %v %v", ok, err)
	}
	// TTL / PTTL / EXPIRETIME conventions: -1 no-expiry, -2 no-key.
	if t2, _ := c.TTL(ctx, "nokey").Result(); t2 >= 0 {
		t.Fatalf("ttl of missing key: %v", t2)
	}
	c.Set(ctx, "plain", "v", 0)
	if t2, _ := c.TTL(ctx, "plain").Result(); t2 >= 0 {
		t.Fatalf("ttl of persistent key: %v", t2)
	}
}

// --- unit/multi.tcl ---

// Runtime errors inside MULTI must NOT poison the queue: EXEC still runs the
// other queued commands, with the failed one surfacing as an in-reply error.
func TestTCLMultiRuntimeErrorKeepsQueue(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	c.Set(ctx, "str", "s", 0)
	if err := c.Do(ctx, "MULTI").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Do(ctx, "SET", "ok", "fine").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Do(ctx, "INCR", "str").Err(); err != nil {
		t.Fatalf("runtime error must still queue: %v", err)
	}
	exec := c.Do(ctx, "EXEC")
	results, err := exec.Slice()
	if err != nil || len(results) != 2 {
		t.Fatalf("exec results: %v %v", results, err)
	}
	if got, _ := c.Get(ctx, "ok").Result(); got != "fine" {
		t.Fatalf("sibling of failed command must run: %q", got)
	}
}

// EXEC leaves the connection in a clean state; a new transaction can start.
func TestTCLMultiStateCleanAfterExec(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	for round := 0; round < 2; round++ {
		if err := c.Do(ctx, "MULTI").Err(); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if err := c.Set(ctx, "r", "v", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Do(ctx, "EXEC").Slice(); err != nil {
			t.Fatal(err)
		}
	}
}
