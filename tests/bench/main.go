// Command bench is a standalone performance/load harness for a running
// redistored instance. It drives N concurrent go-redis clients through a
// configurable workload, measures throughput and latency percentiles, and
// (optionally) verifies that client connections are reaped afterwards.
//
// It complements the in-process data-path micro-benchmarks in
// redistore/bench_sparse_test.go (which measure the storage layer directly):
// this one measures the full network + server + storage stack the way a real
// client sees it, and is also the right harness for surfacing server-side
// races and goroutine leaks under concurrency.
//
// Examples:
//
//	# start a server, then run a 20s mixed load with 64 clients
//	redistored -addr localhost:6380 -dir /tmp/rs
//	go run ./tests/bench -addr localhost:6380 -clients 64 -duration 20s -workload mixed
//
// To check for server-side data races, build the server with -race and run the
// same harness against it; the race detector will fire on any unsynchronised
// shared access under concurrent load:
//
//	go build -race -o bin/redistored-race ./cmd/redistored
//	./bin/redistored-race -addr localhost:6380 -dir /tmp/rs &
//	go run -race ./tests/bench -addr localhost:6380 -clients 64 -duration 20s -workload mixed
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	addrF     = flag.String("addr", "localhost:6380", "redistored address")
	clientsF  = flag.Int("clients", 50, "concurrent client connections")
	durF      = flag.Duration("duration", 10*time.Second, "test duration")
	warmF     = flag.Duration("warmup", 2*time.Second, "warmup duration")
	workloadF = flag.String("workload", "setget", "set|get|setget|hset|hget|pipeline|mixed")
	payloadF  = flag.Int("payload", 64, "value size in bytes")
	keyspaceF = flag.Int("keyspace", 100000, "distinct key count")
	pipeF     = flag.Int("pipeline", 10, "commands per pipeline (pipeline workload)")
	leakF     = flag.Bool("leak", true, "verify connection cleanup after the run")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatalf("bench: %v", err)
	}
}

func run() error {
	ctx := context.Background()
	ctrl := redis.NewClient(&redis.Options{Addr: *addrF, PoolSize: 10})
	defer ctrl.Close()
	if err := ctrl.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("cannot reach %s: %w", *addrF, err)
	}
	if err := ctrl.FlushDB(ctx).Err(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	reads := strings.Contains(*workloadF, "get") || *workloadF == "mixed"
	if reads {
		log.Printf("populating %d keys (workload reads)...", *keyspaceF)
		populate(ctx)
	}

	log.Printf("warmup %s for %s with %d clients...", *workloadF, *warmF, *clientsF)
	runLoad(ctx, *warmF, false)

	log.Printf("running %s for %s with %d clients...", *workloadF, *durF, *clientsF)
	g0 := runtime.NumGoroutine()
	start := time.Now()
	ops, errs, chunks := runLoad(ctx, *durF, true)
	elapsed := time.Since(start)

	total := 0
	for _, ch := range chunks {
		total += len(ch)
	}
	all := make([]time.Duration, 0, total)
	for _, ch := range chunks {
		all = append(all, ch...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		idx := int(p * float64(len(all)-1))
		return all[idx]
	}

	fmt.Println("==== benchmark result ====")
	fmt.Printf("workload    : %s\n", *workloadF)
	fmt.Printf("clients     : %d\n", *clientsF)
	fmt.Printf("duration    : %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("ops         : %d (errors %d)\n", ops, errs)
	if elapsed > 0 {
		fmt.Printf("throughput  : %.0f ops/sec\n", float64(ops)/elapsed.Seconds())
	}
	if len(all) > 0 {
		fmt.Printf("latency p50 : %s\n", pct(0.50))
		fmt.Printf("latency p95 : %s\n", pct(0.95))
		fmt.Printf("latency p99 : %s\n", pct(0.99))
		fmt.Printf("latency p999: %s\n", pct(0.999))
		fmt.Printf("latency max : %s\n", all[len(all)-1])
	}

	if *leakF {
		// Give the server a moment to reap idle connections.
		time.Sleep(500 * time.Millisecond)
		info, err := ctrl.ClientList(ctx).Result()
		live := 0
		if err == nil {
			for _, line := range strings.Split(info, "\n") {
				if strings.TrimSpace(line) != "" {
					live++
				}
			}
		}
		g1 := runtime.NumGoroutine()
		fmt.Println("---- cleanup check ----")
		fmt.Printf("server connections (excl. control): %d\n", live-1)
		if live-1 > 2 {
			fmt.Println("WARNING: server may not be reaping client connections (possible leak)")
		}
		fmt.Printf("client goroutines: before=%d after=%d\n", g0, g1)
		if g1 > g0+*clientsF {
			fmt.Println("WARNING: client-side goroutine count did not return to baseline")
		}
	}
	return nil
}

// runLoad spawns clientsF worker goroutines, runs them for d, and returns
// (ops, errs, latencySamples). Samples are discarded when collect is false.
func runLoad(ctx context.Context, d time.Duration, collect bool) (int64, int64, [][]time.Duration) {
	var ops, errs int64
	ctxRun, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	var (
		mu     sync.Mutex
		chunks [][]time.Duration
		wg     sync.WaitGroup
	)
	for i := 0; i < *clientsF; i++ {
		c := redis.NewClient(&redis.Options{
			Addr:         *addrF,
			PoolSize:     4,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		})
		wg.Add(1)
		go func(id int, c *redis.Client) {
			defer wg.Done()
			defer c.Close()
			local := worker(ctxRun, c, id, &ops, &errs)
			if collect {
				mu.Lock()
				chunks = append(chunks, local)
				mu.Unlock()
			}
		}(i, c)
	}
	wg.Wait()
	return atomic.LoadInt64(&ops), atomic.LoadInt64(&errs), chunks
}

// worker loops until ctx is done and returns a bounded reservoir sample of its
// op latencies (so memory stays fixed regardless of op count).
func worker(ctx context.Context, c *redis.Client, id int, ops, errs *int64) []time.Duration {
	r := rand.New(rand.NewSource(int64(id) + 1))
	val := strings.Repeat("x", *payloadF)
	local := make([]time.Duration, 0, 1<<16)
	var seen int64
	ks := *keyspaceF
	key := func() string { return "bk:" + strconv.Itoa(r.Intn(ks)) }
	for {
		select {
		case <-ctx.Done():
			return local
		default:
		}
		var err error
		t0 := time.Now()
		switch *workloadF {
		case "set":
			err = c.Set(ctx, key(), val, 0).Err()
		case "get":
			_, err = c.Get(ctx, key()).Result()
		case "setget":
			if r.Intn(2) == 0 {
				err = c.Set(ctx, key(), val, 0).Err()
			} else {
				_, err = c.Get(ctx, key()).Result()
			}
		case "hset":
			err = c.HSet(ctx, "bh:"+key(), "f", val).Err()
		case "hget":
			_, err = c.HGet(ctx, "bh:"+key(), "f").Result()
		case "pipeline":
			_, err = c.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for i := 0; i < *pipeF; i++ {
					pipe.Set(ctx, key(), val, 0)
				}
				return nil
			})
		case "mixed":
			switch r.Intn(4) {
			case 0:
				err = c.Set(ctx, key(), val, 0).Err()
			case 1:
				_, err = c.Get(ctx, key()).Result()
			case 2:
				err = c.HSet(ctx, "bh:"+key(), "f", val).Err()
			case 3:
				_, err = c.HGet(ctx, "bh:"+key(), "f").Result()
			}
		default:
			err = fmt.Errorf("unknown workload %q", *workloadF)
		}
		d := time.Since(t0)
		if err != nil {
			atomic.AddInt64(errs, 1)
			continue
		}
		atomic.AddInt64(ops, 1)
		// Reservoir sampling keeps memory bounded.
		seen++
		if int(seen) <= cap(local) {
			local = append(local, d)
		} else {
			j := r.Intn(int(seen))
			if j < len(local) {
				local[j] = d
			}
		}
	}
}

// populate seeds the key space so read workloads hit real data.
func populate(ctx context.Context) {
	const concurrency = 20
	var wg sync.WaitGroup
	perClient := (*keyspaceF + concurrency - 1) / concurrency
	val := strings.Repeat("x", *payloadF)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			cl := redis.NewClient(&redis.Options{Addr: *addrF, PoolSize: 4})
			defer cl.Close()
			end := start + perClient
			if end > *keyspaceF {
				end = *keyspaceF
			}
			for k := start; k < end; k++ {
				kk := "bk:" + strconv.Itoa(k)
				if err := cl.Set(ctx, kk, val, 0).Err(); err != nil {
					return
				}
				if err := cl.HSet(ctx, "bh:"+kk, "f", val).Err(); err != nil {
					return
				}
			}
		}(i * perClient)
	}
	wg.Wait()
}
