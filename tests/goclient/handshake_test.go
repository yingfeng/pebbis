package goclient

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestHandshakeAndBasics: go-redis's default connection negotiation (HELLO 3
// with RESP2 fallback, CLIENT SETINFO) must succeed against pebbisd, and
// the basic round trips must work on both negotiated and forced-RESP2 clients.
func TestHandshakeAndBasics(t *testing.T) {
	addr := startServer(t)

	// Client defaults: whatever negotiation go-redis does must just work.
	def := defaultClient(t, addr)
	if err := def.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("default client ping: %v", err)
	}
	if err := def.Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got, err := def.Get(context.Background(), "k").Result(); err != nil || got != "v" {
		t.Fatalf("get: %q %v", got, err)
	}

	// Forced RESP2.
	c := newClient(t, addr)
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("resp2 ping: %v", err)
	}
	if err := c.Echo(context.Background(), "hi").Err(); err != nil {
		t.Fatalf("echo: %v", err)
	}

	// Connection-scoped SELECT.
	sel := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2, DB: 1})
	defer sel.Close()
	if err := sel.Set(context.Background(), "db1key", "x", 0).Err(); err != nil {
		t.Fatalf("set db1: %v", err)
	}
	if err := c.Get(context.Background(), "db1key").Err(); err != redis.Nil {
		t.Fatalf("db isolation: want redis.Nil, got %v", err)
	}
}

// TestPoolConcurrency: 50 goroutines hammering the pool must all succeed -
// this is the production access pattern the RESP suite cannot cover.
func TestPoolConcurrency(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key"
			for j := 0; j < 20; j++ {
				if err := c.Set(ctx, key, i, 0).Err(); err != nil {
					errs <- err
					return
				}
				if _, err := c.Get(ctx, key).Result(); err != nil {
					errs <- err
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent op: %v", err)
	}
}

// TestTTLAndExpiry: TTL semantics through the typed API.
func TestTTLAndExpiry(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	if err := c.Set(ctx, "ttl", "v", 100*time.Second).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	ttl, err := c.TTL(ctx, "ttl").Result()
	if err != nil || ttl <= 0 || ttl > 100*time.Second {
		t.Fatalf("ttl: %v %v", ttl, err)
	}
	if err := c.Persist(ctx, "ttl").Err(); err != nil {
		t.Fatalf("persist: %v", err)
	}
	if got, err := c.TTL(ctx, "ttl").Result(); err != nil || got > 0 {
		t.Fatalf("ttl after persist: %v %v", got, err)
	}
	// Expired keys vanish and report -2.
	if err := c.Set(ctx, "gone", "v", 10*time.Millisecond).Err(); err != nil {
		t.Fatalf("set gone: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := c.Get(ctx, "gone").Err(); err != redis.Nil {
		t.Fatalf("expired key: want redis.Nil, got %v", err)
	}
	if ttl, _ := c.TTL(ctx, "gone").Result(); ttl >= 0 {
		t.Fatalf("ttl of missing key: %v", ttl)
	}
}

// TestErrorSemantics: the typed errors clients branch on.
func TestErrorSemantics(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	if err := c.Get(ctx, "missing").Err(); err != redis.Nil {
		t.Fatalf("missing key: want redis.Nil, got %v", err)
	}
	if err := c.HSet(ctx, "h", "f", "v").Err(); err != nil {
		t.Fatalf("hset: %v", err)
	}
	if err := c.Get(ctx, "h").Err(); err == nil || err == redis.Nil {
		t.Fatalf("wrongtype get: want WRONGTYPE error, got %v", err)
	}
	if err := c.Set(ctx, "s", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Incr(ctx, "s").Result(); err == nil {
		t.Fatalf("incr on string value: want error, got %d", n)
	}
}
