// Package conctest runs a standalone, dependency-free concurrency soak test
// against a running pebbisd instance using the go-redis client.
//
// It deliberately lives in its own package so it does NOT pull in the
// tests/goclient suite's shared setup. That lets us exercise pebbisd with
// the real go-redis client without standing up that topology.
//
// Run with:
//
//	REDIS_ADDR=localhost:6380 go test -race -v ./tests/conctest/
//
// The server address defaults to localhost:6380 (the instance started for the
// soak); override with REDIS_ADDR.
package conctest

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

func addr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "localhost:6380"
}

func newClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr(), PoolSize: 20})
}

// scanCount walks a SCAN-style cursor to 0 and returns the total number of
// array elements returned across all pages. go-redis v9's HScan/SScan/ZScan take
// a uint64 cursor and return (newCursor uint64, keys []string, err error).
func scanCount(t *testing.T, fn func(cur uint64) (uint64, []string, error)) int {
	t.Helper()
	total := 0
	cur := uint64(0)
	for {
		nc, els, err := fn(cur)
		if err != nil {
			t.Fatalf("scan page: %v", err)
		}
		total += len(els)
		cur = nc
		if cur == 0 {
			return total
		}
	}
}

// TestPebbisConcurrentSoak hammers a single pebbisd with many goroutines
// doing general commands plus full HSCAN/SSCAN/ZSCAN iterations honouring COUNT,
// asserting every client observes the expected totals (i.e. the server's
// pagination cursor is consistent under concurrent access).
func TestPebbisConcurrentSoak(t *testing.T) {
	ctx := context.Background()
	c := newClient()
	// The soak runs against a live pebbisd (see package doc). Skip when no
	// server is reachable so `make test` stays green in environments without one;
	// run `make soak` (or set REDIS_ADDR) to actually execute the suite. This
	// mirrors the skip-when-unavailable convention used by tests/miniredis.
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("pebbisd not reachable at %s: %v (run `make soak` or set REDIS_ADDR)", addr(), err)
	}
	if err := c.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	const seeded = 100
	for i := 0; i < seeded; i++ {
		k := "f" + pad(i)
		if err := c.HSet(ctx, "h", k, "v").Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.SAdd(ctx, "s", "m"+pad(i)).Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.ZAdd(ctx, "z", redis.Z{Score: float64(i), Member: "m" + pad(i)}).Err(); err != nil {
			t.Fatal(err)
		}
	}

	const workers = 20
	const iters = 200
	// Pre-seed the per-worker fields/members so the collection's MEMBERSHIP is
	// constant for the whole test. Workers only ever change *values* of existing
	// fields (never add or remove one), so a full HSCAN/SSCAN/ZSCAN must always
	// return the same total. If a sparse write committed elements and header in
	// two separate batches, a concurrent scan could observe a torn (half-written)
	// collection and the total would differ from the stable expected value.
	for id := 0; id < workers; id++ {
		if err := c.HSet(ctx, "h", "w"+strconv.Itoa(id), "").Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.SAdd(ctx, "s", "x"+strconv.Itoa(id)).Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.ZAdd(ctx, "z", redis.Z{Score: 0, Member: "y" + strconv.Itoa(id)}).Err(); err != nil {
			t.Fatal(err)
		}
	}

	// Stable totals for the whole run: the membership never changes.
	wantFields := seeded + workers // HSCAN returns 2 elements per field
	wantSet := seeded + workers    // SSCAN returns 1 element per member
	wantZ := seeded + workers      // ZSCAN returns 2 elements per member

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cl := newClient()
			defer cl.Close()
			for i := 0; i < iters; i++ {
				if err := cl.Incr(ctx, "counter").Err(); err != nil {
					errCh <- err
					return
				}
				key := fmt.Sprintf("k:%d:%d", id, i)
				if err := cl.Set(ctx, key, "v", 0).Err(); err != nil {
					errCh <- err
					return
				}
				if got, err := cl.Get(ctx, key).Result(); err != nil || got != "v" {
					errCh <- fmt.Errorf("get %s: %v %q", key, err, got)
					return
				}
				if err := cl.HSet(ctx, "h", "w"+strconv.Itoa(id), strconv.Itoa(i)).Err(); err != nil {
					errCh <- err
					return
				}
				if err := cl.SAdd(ctx, "s", "x"+strconv.Itoa(id)).Err(); err != nil {
					errCh <- err
					return
				}
				if err := cl.ZAdd(ctx, "z", redis.Z{Score: float64(i), Member: "y" + strconv.Itoa(id)}).Err(); err != nil {
					errCh <- err
					return
				}

				// Pagination must be stable under concurrency: a full walk
				// collecting every page should always total the same.
				hn := scanCount(t, func(cur uint64) (uint64, []string, error) {
					els, nc, err := cl.HScan(ctx, "h", cur, "", 10).Result()
					return nc, els, err
				})
				if hn != wantFields*2 {
					errCh <- fmt.Errorf("hscan total %d, want %d", hn, wantFields*2)
					return
				}
				sn := scanCount(t, func(cur uint64) (uint64, []string, error) {
					els, nc, err := cl.SScan(ctx, "s", cur, "", 10).Result()
					return nc, els, err
				})
				if sn != wantSet {
					errCh <- fmt.Errorf("sscan total %d, want %d", sn, wantSet)
					return
				}
				zn := scanCount(t, func(cur uint64) (uint64, []string, error) {
					els, nc, err := cl.ZScan(ctx, "z", cur, "", 10).Result()
					return nc, els, err
				})
				if zn != wantZ*2 {
					errCh <- fmt.Errorf("zscan total %d, want %d", zn, wantZ*2)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	var soakErr error
	for err := range errCh {
		if err != nil {
			soakErr = err
		}
	}

	cnt, err := c.Get(ctx, "counter").Int64()
	if err != nil {
		t.Fatal(err)
	}
	// Final single-threaded walk: distinguishes a read-side race (which the RWMutex
	// should have closed) from a genuine write loss (element keys missing in Pebble).
	hFinal := scanCount(t, func(cur uint64) (uint64, []string, error) {
		els, nc, err := c.HScan(ctx, "h", cur, "", 10).Result()
		return nc, els, err
	})
	hv, err := c.HGetAll(ctx, "h").Result()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("FINAL single-threaded: counter=%d, hscan=%d, hgetall=%d (want hscan/hgetall=%d)",
		cnt, hFinal, len(hv), wantFields*2)
	if cnt != int64(workers*iters) {
		t.Errorf("counter=%d, want %d", cnt, workers*iters)
	}
	if soakErr != nil {
		t.Fatalf("concurrent op failed: %v", soakErr)
	}
	t.Logf("ok: %d workers x %d iters, counter=%d, hscan=%d sscan=%d zscan=%d",
		workers, iters, cnt, wantFields*2, wantSet, wantZ*2)
}

func pad(i int) string {
	return fmt.Sprintf("%02d", i)
}
