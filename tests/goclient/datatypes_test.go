package goclient

import (
	"context"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestHashTypedAPI covers the hash lifecycle through go-redis's typed API.
func TestHashTypedAPI(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	if err := c.HSet(ctx, "h", map[string]interface{}{
		"f1": "v1", "f2": "v2",
	}).Err(); err != nil {
		t.Fatalf("hset: %v", err)
	}
	all, err := c.HGetAll(ctx, "h").Result()
	if err != nil || !reflect.DeepEqual(all, map[string]string{"f1": "v1", "f2": "v2"}) {
		t.Fatalf("hgetall: %v %v", all, err)
	}
	if v, err := c.HGet(ctx, "h", "f1").Result(); err != nil || v != "v1" {
		t.Fatalf("hget: %q %v", v, err)
	}
	if err := c.HGet(ctx, "h", "missing").Err(); err != redis.Nil {
		t.Fatalf("hget missing: %v", err)
	}
	if n, err := c.HIncrBy(ctx, "h", "n", 5).Result(); err != nil || n != 5 {
		t.Fatalf("hincrby: %d %v", n, err)
	}
	if n, err := c.HDel(ctx, "h", "f1").Result(); err != nil || n != 1 {
		t.Fatalf("hdel: %d %v", n, err)
	}
}

// TestListTypedAPI covers push/pop/range plus the missing-key conventions.
func TestListTypedAPI(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	for _, v := range []string{"a", "b", "c"} {
		if err := c.RPush(ctx, "l", v).Err(); err != nil {
			t.Fatalf("rpush: %v", err)
		}
	}
	if n, err := c.LLen(ctx, "l").Result(); err != nil || n != 3 {
		t.Fatalf("llen: %d %v", n, err)
	}
	if got, err := c.LRange(ctx, "l", 0, -1).Result(); err != nil || !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("lrange: %v %v", got, err)
	}
	// LPop with count replies an array; missing key replies nil.
	if v, err := c.LPop(ctx, "l").Result(); err != nil || v != "a" {
		t.Fatalf("lpop: %q %v", v, err)
	}
	if err := c.LPop(ctx, "missing").Err(); err != redis.Nil {
		t.Fatalf("lpop missing: %v", err)
	}
	// LTrim deleting the whole list removes the key.
	if err := c.LTrim(ctx, "l", 5, 10).Err(); err != nil {
		t.Fatalf("ltrim: %v", err)
	}
	if n, err := c.Exists(ctx, "l").Result(); err != nil || n != 0 {
		t.Fatalf("exists after ltrim: %d %v", n, err)
	}
}

// TestSetTypedAPI covers set membership and aggregate ops.
func TestSetTypedAPI(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	c.SAdd(ctx, "a", "1", "2", "3")
	c.SAdd(ctx, "b", "2", "3", "4")
	if n, err := c.SCard(ctx, "a").Result(); err != nil || n != 3 {
		t.Fatalf("scard: %d %v", n, err)
	}
	inter, err := c.SInter(ctx, "a", "b").Result()
	if err != nil || !reflect.DeepEqual(inter, []string{"2", "3"}) {
		t.Fatalf("sinter: %v %v", inter, err)
	}
	if ok, err := c.SIsMember(ctx, "a", "9").Result(); err != nil || ok {
		t.Fatalf("sismember: %v %v", ok, err)
	}
	if n, err := c.SRem(ctx, "a", "1").Result(); err != nil || n != 1 {
		t.Fatalf("srem: %d %v", n, err)
	}
}

// TestZSetTypedAPI covers scores, ranks and rev-range ordering.
func TestZSetTypedAPI(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	members := []redis.Z{
		{Score: 1, Member: "one"},
		{Score: 2, Member: "two"},
		{Score: 3, Member: "three"},
	}
	if n, err := c.ZAdd(ctx, "z", members...).Result(); err != nil || n != 3 {
		t.Fatalf("zadd: %d %v", n, err)
	}
	// ZRANGE WITHSCORES is score-ordered.
	ran, err := c.ZRangeWithScores(ctx, "z", 0, -1).Result()
	if err != nil || len(ran) != 3 || ran[0].Member != "one" || ran[2].Member != "three" {
		t.Fatalf("zrange: %v %v", ran, err)
	}
	if r, err := c.ZRank(ctx, "z", "two").Result(); err != nil || r != 1 {
		t.Fatalf("zrank: %d %v", r, err)
	}
	if s, err := c.ZScore(ctx, "z", "two").Result(); err != nil || s != 2 {
		t.Fatalf("zscore: %v %v", s, err)
	}
	// Rev range returns highest first.
	rev, err := c.ZRevRange(ctx, "z", 0, 0).Result()
	if err != nil || len(rev) != 1 || rev[0] != "three" {
		t.Fatalf("zrevrange: %v %v", rev, err)
	}
	// NX must not overwrite.
	if n, err := c.ZAddNX(ctx, "z", redis.Z{Score: 99, Member: "one"}).Result(); err != nil || n != 0 {
		t.Fatalf("zaddnx: %d %v", n, err)
	}
}

// TestScanIterator: cursor iteration through go-redis's ScanIterator must
// return every key exactly once.
func TestScanIterator(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	want := map[string]bool{}
	for i := 0; i < 50; i++ {
		key := "key:" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		if err := c.Set(ctx, key, i, 0).Err(); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
		want[key] = true
	}
	seen := map[string]bool{}
	iter := c.Scan(ctx, 0, "key:*", 10).Iterator()
	for iter.Next(ctx) {
		seen[iter.Val()] = true
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	for k := range want {
		if !seen[k] {
			t.Errorf("scan missed %s", k)
		}
	}
}
