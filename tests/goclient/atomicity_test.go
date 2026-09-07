package goclient

// Concurrency-correctness gate: Redis promises that commands execute
// atomically, so concurrent writers to one key must never lose an update.
// These tests failed before write-command serialisation was added.

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestConcurrentAtomicity(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()
	const workers, ops = 20, 100

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				if err := c.Incr(ctx, "counter").Err(); err != nil {
					t.Error(err)
					return
				}
				// Each worker owns distinct hash fields / list entries.
				if err := c.HSet(ctx, "h", "f"+strconv.Itoa(w)+"_"+strconv.Itoa(i), "v").Err(); err != nil {
					t.Error(err)
					return
				}
				if err := c.RPush(ctx, "l", w).Err(); err != nil {
					t.Error(err)
					return
				}
				if err := c.ZAdd(ctx, "z", redis.Z{Score: float64(w), Member: "m" + strconv.Itoa(w) + "_" + strconv.Itoa(i)}).Err(); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if n, err := c.Get(ctx, "counter").Int64(); err != nil || n != workers*ops {
		t.Errorf("counter: got %d, want %d (lost INCR updates)", n, workers*ops)
	}
	if n, err := c.HLen(ctx, "h").Result(); err != nil || n != workers*ops {
		t.Errorf("hlen: got %d, want %d (lost HSET fields)", n, workers*ops)
	}
	if n, err := c.LLen(ctx, "l").Result(); err != nil || n != workers*ops {
		t.Errorf("llen: got %d, want %d (lost RPUSH entries)", n, workers*ops)
	}
	if n, err := c.ZCard(ctx, "z").Result(); err != nil || n != workers*ops {
		t.Errorf("zcard: got %d, want %d (lost ZADD members)", n, workers*ops)
	}
}
