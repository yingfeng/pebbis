package goclient

// Benchmarks modelled on go-redis' own bench_test.go: same shape (b.RunParallel
// over a pooled client), pointed at a live pebbisd instance. Run with:
//
//	go test ./tests/goclient -bench . -benchmem -benchtime 1s

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// benchClient pools `poolSize` connections against the server.
func benchClient(b *testing.B, addr string, poolSize int) *redis.Client {
	b.Helper()
	c := redis.NewClient(&redis.Options{
		Addr:         addr,
		Protocol:     2,
		PoolSize:     poolSize,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	})
	b.Cleanup(func() { _ = c.Close() })
	return c
}

// startBenchServer pairs a server with a pooled default client.
func startBenchServer(b *testing.B, poolSize int) (context.Context, *redis.Client) {
	b.Helper()
	addr := startServer(b)
	return context.Background(), benchClient(b, addr, poolSize)
}

// BenchmarkPing measures pure round-trip overhead (no storage work).
func BenchmarkPing(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := c.Ping(ctx).Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkGetNil measures the miss path (redis.Nil short-circuit).
func BenchmarkGetNil(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := c.Get(ctx, "nonexistent").Err(); err != redis.Nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSetString mirrors go-redis' value-size x pool-size matrix.
func BenchmarkSetString(b *testing.B) {
	cases := []struct {
		pool, value int
	}{
		{10, 64}, {10, 1024}, {10, 64 * 1024},
		{50, 64}, {50, 1024}, {50, 64 * 1024},
	}
	for _, tc := range cases {
		b.Run(fmt.Sprintf("pool=%d value=%dB", tc.pool, tc.value), func(b *testing.B) {
			ctx, c := startBenchServer(b, tc.pool)
			value := strings.Repeat("x", tc.value)
			b.SetBytes(int64(tc.value))
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := c.Set(ctx, "key", value, 0).Err(); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkSetGet alternates a write and a read on one hot key: the classic
// cache churn pattern.
func BenchmarkSetGet(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	value := strings.Repeat("v", 1024)
	b.SetBytes(2 * 1024)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := c.Set(ctx, "hot", value, 0).Err(); err != nil {
				b.Fatal(err)
			}
			if _, err := c.Get(ctx, "hot").Result(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkIncr contends on a single counter: exercises the atomic path under
// pool-level parallelism.
func BenchmarkIncr(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := c.Incr(ctx, "counter").Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPipeline submits 10-command batches.
func BenchmarkPipeline(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pipe := c.Pipeline()
			for i := 0; i < 10; i++ {
				pipe.Set(ctx, "pipe:"+strconv.Itoa(i), i, 0)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkTxPipeline wraps the same batch in MULTI/EXEC.
func BenchmarkTxPipeline(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pipe := c.TxPipeline()
			for i := 0; i < 5; i++ {
				pipe.Set(ctx, "tx:"+strconv.Itoa(i), i, 0)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkHSetHGet covers the hash aggregate path.
func BenchmarkHSetHGet(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "h:" + strconv.Itoa(i%16)
			field := "f" + strconv.Itoa(i)
			if err := c.HSet(ctx, key, field, "v").Err(); err != nil {
				b.Fatal(err)
			}
			if _, err := c.HGet(ctx, key, field).Result(); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkListPushPop exercises the list aggregate with a push/pop pair.
func BenchmarkListPushPop(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "l:" + strconv.Itoa(i%16)
			if err := c.RPush(ctx, key, i).Err(); err != nil {
				b.Fatal(err)
			}
			if _, err := c.LPop(ctx, key).Result(); err != nil && err != redis.Nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkLRANGE100 reads back a 100-element window, the feed-style pattern.
func BenchmarkLRANGE100(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	for i := 0; i < 100; i++ {
		c.RPush(ctx, "feed", i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if got, err := c.LRange(ctx, "feed", 0, 99).Result(); err != nil || len(got) != 100 {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkZAdd covers the score-index update path.
func BenchmarkZAdd(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := c.ZAdd(ctx, "ranked", redis.Z{
				Score: float64(i), Member: "m" + strconv.Itoa(i%64),
			}).Err(); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkXAdd covers the stream append path (ID generation + entry write).
func BenchmarkXAdd(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := c.XAdd(ctx, &redis.XAddArgs{
				Stream: "events",
				Values: map[string]interface{}{"i": i, "payload": strings.Repeat("p", 128)},
			}).Err(); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkMGet16 batches 16 reads into one round trip.
func BenchmarkMGet16(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	for i := 0; i < 16; i++ {
		c.Set(ctx, "mg:"+strconv.Itoa(i), i, 0)
	}
	keys := make([]string, 16)
	for i := range keys {
		keys[i] = "mg:" + strconv.Itoa(i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := c.MGet(ctx, keys...).Result(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSetGoroutines is go-redis' burst test: 1000 concurrent SETs.
func BenchmarkSetGoroutines(b *testing.B) {
	ctx, c := startBenchServer(b, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for j := 0; j < 1000; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := c.Set(ctx, "hello", "world", 0).Err(); err != nil {
					b.Error(err)
				}
			}()
		}
		wg.Wait()
	}
}
