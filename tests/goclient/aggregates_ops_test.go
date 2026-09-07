package goclient

// Aggregate (hash/list/set/zset) operations ported from miniredis' command
// tests, expressed through go-redis' typed API.

import (
	"context"
	"reflect"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestHashVariants(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	if ok, err := c.HSetNX(ctx, "h", "f", "v1").Result(); err != nil || !ok {
		t.Fatalf("hsetnx: %v %v", ok, err)
	}
	if ok, err := c.HSetNX(ctx, "h", "f", "v2").Result(); err != nil || ok {
		t.Fatalf("hsetnx existing: %v %v", ok, err)
	}
	if ok, err := c.HExists(ctx, "h", "f").Result(); err != nil || !ok {
		t.Fatalf("hexists: %v %v", ok, err)
	}
	c.HSet(ctx, "h", "g", "w")
	if keys, err := c.HKeys(ctx, "h").Result(); err != nil || len(keys) != 2 {
		t.Fatalf("hkeys: %v %v", keys, err)
	}
	if vals, err := c.HVals(ctx, "h").Result(); err != nil || len(vals) != 2 {
		t.Fatalf("hvals: %v %v", vals, err)
	}
	if n, err := c.HStrLen(ctx, "h", "f").Result(); err != nil || n != 2 {
		t.Fatalf("hstrlen: %d %v", n, err)
	}
	if got, err := c.HMGet(ctx, "h", "f", "nope").Result(); err != nil || got[0] != "v1" || got[1] != nil {
		t.Fatalf("hmget: %v %v", got, err)
	}
	if n, err := c.HRandField(ctx, "h", 1).Result(); err != nil || len(n) != 1 {
		t.Fatalf("hrandfield: %v %v", n, err)
	}
}

func TestListVariants(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	c.RPush(ctx, "l", "a", "b", "c", "a")
	if n, err := c.LPushX(ctx, "l", "head").Result(); err != nil || n != 5 {
		t.Fatalf("lpushx: %d %v", n, err)
	}
	// RPUSHX on a missing key must not create it.
	if n, err := c.RPushX(ctx, "nolist", "x").Result(); err != nil || n != 0 {
		t.Fatalf("rpushx missing: %d %v", n, err)
	}
	if got, err := c.LIndex(ctx, "l", 1).Result(); err != nil || got != "a" {
		t.Fatalf("lindex: %q %v", got, err)
	}
	if n, err := c.LRem(ctx, "l", 1, "a").Result(); err != nil || n != 1 {
		t.Fatalf("lrem: %d %v", n, err)
	}
	if err := c.LSet(ctx, "l", 0, "HEAD").Err(); err != nil {
		t.Fatalf("lset: %v", err)
	}
	if n, err := c.LPos(ctx, "l", "HEAD", redis.LPosArgs{}).Result(); err != nil || n != 0 {
		t.Fatalf("lpos: %d %v", n, err)
	}
	// LMOVE rotates between lists.
	c.RPush(ctx, "l2", "z")
	if err := c.LMove(ctx, "l", "l2", "LEFT", "RIGHT").Err(); err != nil {
		t.Fatalf("lmove: %v", err)
	}
	if got, _ := c.LRange(ctx, "l2", -1, -1).Result(); len(got) != 1 || got[0] != "HEAD" {
		t.Fatalf("after lmove: %v", got)
	}
}

func TestSetVariants(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	c.SAdd(ctx, "s1", "a", "b", "c")
	c.SAdd(ctx, "s2", "b", "c", "d")
	if diff, err := c.SDiff(ctx, "s1", "s2").Result(); err != nil || !reflect.DeepEqual(diff, []string{"a"}) {
		t.Fatalf("sdiff: %v %v", diff, err)
	}
	if n, err := c.SDiffStore(ctx, "dst", "s1", "s2").Result(); err != nil || n != 1 {
		t.Fatalf("sdiffstore: %d %v", n, err)
	}
	if uni, err := c.SUnion(ctx, "s1", "s2").Result(); err != nil || len(uni) != 4 {
		t.Fatalf("sunion: %v %v", uni, err)
	}
	if n, err := c.SUnionStore(ctx, "udst", "s1", "s2").Result(); err != nil || n != 4 {
		t.Fatalf("sunionstore: %d %v", n, err)
	}
	if ok, err := c.SMIsMember(ctx, "s1", "a", "zz").Result(); err != nil || !ok[0] || ok[1] {
		t.Fatalf("smismember: %v %v", ok, err)
	}
	if ok, err := c.SMove(ctx, "s1", "s2", "a").Result(); err != nil || !ok {
		t.Fatalf("smove: %v %v", ok, err)
	}
	if mem, err := c.SPop(ctx, "s1").Result(); err != nil || mem == "" {
		t.Fatalf("spop: %q %v", mem, err)
	}
	if mems, err := c.SPopN(ctx, "s2", 2).Result(); err != nil || len(mems) != 2 {
		t.Fatalf("spopn: %v %v", mems, err)
	}
	if n, err := c.SCard(ctx, "s1").Result(); err != nil || n != 1 {
		t.Fatalf("scard after pops: %d %v", n, err)
	}
}

func TestZSetVariants(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	c.ZAdd(ctx, "z",
		redis.Z{Score: 1, Member: "a"},
		redis.Z{Score: 2, Member: "b"},
		redis.Z{Score: 3, Member: "c"},
		redis.Z{Score: 4, Member: "d"},
	)
	// ZPOPMIN / ZPOPMAX.
	if popped, err := c.ZPopMin(ctx, "z").Result(); err != nil || popped[0].Member != "a" {
		t.Fatalf("zpopmin: %v %v", popped, err)
	}
	if popped, err := c.ZPopMax(ctx, "z").Result(); err != nil || popped[0].Member != "d" {
		t.Fatalf("zpopmax: %v %v", popped, err)
	}
	// ZCOUNT / ZINCRBY.
	if n, err := c.ZCount(ctx, "z", "2", "3").Result(); err != nil || n != 2 {
		t.Fatalf("zcount: %d %v", n, err)
	}
	if s, err := c.ZIncrBy(ctx, "z", 10, "b").Result(); err != nil || s != 12 {
		t.Fatalf("zincrby: %v %v", s, err)
	}
	// ZREVRANK / ZREMRANGEBYRANK / ZREMRANGEBYSCORE.
	if r, err := c.ZRevRank(ctx, "z", "b").Result(); err != nil || r != 0 {
		t.Fatalf("zrevrank: %d %v", r, err)
	}
	if n, err := c.ZRemRangeByScore(ctx, "z", "3", "3").Result(); err != nil || n != 1 {
		t.Fatalf("zremrangebyscore: %d %v", n, err)
	}
	if n, err := c.ZRemRangeByRank(ctx, "z", 0, 0).Result(); err != nil || n != 1 {
		t.Fatalf("zremrangebyrank: %d %v", n, err)
	}
	// "b" was already removed by ZREMRANGEBYRANK (it had the top score).
	if n, err := c.ZRem(ctx, "z", "b").Result(); err != nil || n != 0 {
		t.Fatalf("zrem: %d %v", n, err)
	}
	if n, err := c.ZCard(ctx, "z").Result(); err != nil || n != 0 {
		t.Fatalf("zcard: %d %v", n, err)
	}
	// ZRANDMEMBER.
	c.ZAdd(ctx, "zr", redis.Z{Score: 1, Member: "x"}, redis.Z{Score: 2, Member: "y"})
	if m, err := c.ZRandMember(ctx, "zr", 1).Result(); err != nil || len(m) != 1 {
		t.Fatalf("zrandmember: %v %v", m, err)
	}
	// ZUNIONSTORE.
	c.ZAdd(ctx, "z2", redis.Z{Score: 5, Member: "x"})
	if n, err := c.ZUnionStore(ctx, "zu", &redis.ZStore{Keys: []string{"zr", "z2"}}).Result(); err != nil || n != 2 {
		t.Fatalf("zunionstore: %d %v", n, err)
	}
	if s, err := c.ZScore(ctx, "zu", "x").Result(); err != nil || s != 6 {
		t.Fatalf("unioned score: %v %v", s, err)
	}
	// ZDIFFSTORE.
	c.ZAdd(ctx, "z3", redis.Z{Score: 1, Member: "x"}, redis.Z{Score: 1, Member: "q"})
	if n, err := c.ZDiffStore(ctx, "zd", "z3", "z2").Result(); err != nil || n != 1 {
		t.Fatalf("zdiffstore: %d %v", n, err)
	}
}
