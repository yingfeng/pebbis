package goclient

// String and key operations ported from miniredis' cmd_string/generic tests
// and go-redis' command specs, expressed through the typed client API.

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestStringOperations(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	// APPEND / STRLEN
	if n, err := c.Append(ctx, "s", "Hello").Result(); err != nil || n != 5 {
		t.Fatalf("append1: %d %v", n, err)
	}
	if n, err := c.Append(ctx, "s", " World").Result(); err != nil || n != 11 {
		t.Fatalf("append2: %d %v", n, err)
	}
	if n, err := c.StrLen(ctx, "s").Result(); err != nil || n != 11 {
		t.Fatalf("strlen: %d %v", n, err)
	}

	// GETRANGE / SETRANGE
	if got, err := c.GetRange(ctx, "s", 0, 4).Result(); err != nil || got != "Hello" {
		t.Fatalf("getrange: %q %v", got, err)
	}
	if got, err := c.GetRange(ctx, "s", -5, -1).Result(); err != nil || got != "World" {
		t.Fatalf("getrange neg: %q %v", got, err)
	}
	if n, err := c.SetRange(ctx, "s", 6, "Redis").Result(); err != nil || n != 11 {
		t.Fatalf("setrange: %d %v", n, err)
	}
	if got, _ := c.Get(ctx, "s").Result(); got != "Hello Redis" {
		t.Fatalf("after setrange: %q", got)
	}

	// Counter family.
	c.Set(ctx, "n", "10", 0)
	if v, err := c.IncrBy(ctx, "n", 5).Result(); err != nil || v != 15 {
		t.Fatalf("incrby: %d %v", v, err)
	}
	if v, err := c.DecrBy(ctx, "n", 3).Result(); err != nil || v != 12 {
		t.Fatalf("decrby: %d %v", v, err)
	}
	if v, err := c.IncrByFloat(ctx, "n", 0.5).Result(); err != nil || v != 12.5 {
		t.Fatalf("incrbyfloat: %v %v", v, err)
	}
	if err := c.Incr(ctx, "s").Err(); err == nil {
		t.Fatal("incr on non-integer must error")
	}

	// MSET / MSETNX / MGET.
	if err := c.MSet(ctx, "m1", "a", "m2", "b").Err(); err != nil {
		t.Fatalf("mset: %v", err)
	}
	if ok, err := c.MSetNX(ctx, "m1", "x", "m3", "c").Result(); err != nil || ok {
		t.Fatalf("msetnx with existing key: %v %v", ok, err)
	}
	vals, err := c.MGet(ctx, "m1", "m2", "missing").Result()
	if err != nil || vals[0] != "a" || vals[1] != "b" || vals[2] != nil {
		t.Fatalf("mget: %v %v", vals, err)
	}

	// SETNX / GETSET / GETDEL.
	if ok, err := c.SetNX(ctx, "nx", "1", 0).Result(); err != nil || !ok {
		t.Fatalf("setnx: %v %v", ok, err)
	}
	if ok, err := c.SetNX(ctx, "nx", "2", 0).Result(); err != nil || ok {
		t.Fatalf("setnx again: %v %v", ok, err)
	}
	old, err := c.GetSet(ctx, "nx", "3").Result()
	if err != nil || old != "1" {
		t.Fatalf("getset: %q %v", old, err)
	}
	gone, err := c.GetDel(ctx, "nx").Result()
	if err != nil || gone != "3" {
		t.Fatalf("getdel: %q %v", gone, err)
	}
	if err := c.Get(ctx, "nx").Err(); err != redis.Nil {
		t.Fatalf("key must be gone: %v", err)
	}
}

func TestSetExpiryVariants(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	if err := c.SetArgs(ctx, "a", "v", redis.SetArgs{Mode: "NX", TTL: 60 * time.Second}).Err(); err != nil {
		t.Fatalf("setargs nx+ex: %v", err)
	}
	if err := c.SetArgs(ctx, "b", "v", redis.SetArgs{Mode: "XX", KeepTTL: false}).Err(); err == nil {
		t.Fatal("setargs xx on missing key must report Nil reply")
	}
	// SETEX / PSETEX.
	if err := c.SetEx(ctx, "se", "v", 30*time.Second).Err(); err != nil {
		t.Fatalf("setex: %v", err)
	}
	if ttl, _ := c.TTL(ctx, "se").Result(); ttl <= 0 {
		t.Fatalf("setex ttl: %v", ttl)
	}
	// EXPIREAT / PEXPIREAT in the past deletes the key.
	c.Set(ctx, "past", "v", 0)
	if err := c.ExpireAt(ctx, "past", time.Now().Add(-time.Hour)).Err(); err != nil {
		t.Fatalf("expireat: %v", err)
	}
	if err := c.Get(ctx, "past").Err(); err != redis.Nil {
		t.Fatalf("past expire must delete: %v", err)
	}
	// EXPIRETIME / PEXPIRETIME.
	future := time.Now().Add(2 * time.Minute)
	c.Set(ctx, "et", "v", 0)
	c.ExpireAt(ctx, "et", future)
	// ExpireTime replies Unix seconds surfaced as a Duration.
	if got, err := c.ExpireTime(ctx, "et").Result(); err != nil || int64(got.Seconds()) != future.Unix() {
		t.Fatalf("expiretime: %v want %v (err %v)", got.Seconds(), future.Unix(), err)
	}
}

func TestKeyLifecycle(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	c.Set(ctx, "k1", "v", 0)
	// RENAME / RENAMENX.
	if err := c.Rename(ctx, "k1", "k2").Err(); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if ok, err := c.RenameNX(ctx, "k2", "k1").Result(); err != nil || !ok {
		t.Fatalf("renamenx: %v %v", ok, err)
	}
	// TYPE.
	if typ, err := c.Type(ctx, "k1").Result(); err != nil || typ != "string" {
		t.Fatalf("type: %q %v", typ, err)
	}
	// TOUCH.
	c.Set(ctx, "k3", "v", 0)
	if n, err := c.Touch(ctx, "k1", "k3", "nope").Result(); err != nil || n != 2 {
		t.Fatalf("touch: %d %v", n, err)
	}
	// UNLINK behaves like DEL.
	if n, err := c.Unlink(ctx, "k1").Result(); err != nil || n != 1 {
		t.Fatalf("unlink: %d %v", n, err)
	}
	// COPY with REPLACE.
	c.HSet(ctx, "src", "f", "v")
	if n, err := c.Copy(ctx, "src", "dst", 0, true).Result(); err != nil || n != 1 {
		t.Fatalf("copy: %d %v", n, err)
	}
	if n, err := c.HLen(ctx, "dst").Result(); err != nil || n != 1 {
		t.Fatalf("copied hash: %d %v", n, err)
	}
	// MOVE across DBs.
	if ok, err := c.Move(ctx, "k3", 1).Result(); err != nil || !ok {
		t.Fatalf("move: %v %v", ok, err)
	}
	if n, err := c.Exists(ctx, "k3").Result(); err != nil || n != 0 {
		t.Fatalf("moved key must be gone: %d %v", n, err)
	}
	// FLUSHDB / FLUSHALL.
	c.Set(ctx, "flushme", "v", 0)
	if err := c.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	if n, _ := c.DBSize(ctx).Result(); n != 0 {
		t.Fatalf("dbsize after flush: %d", n)
	}
}
