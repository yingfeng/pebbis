package pebbis

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pebbis/pebbis/config"
	"github.com/pebbis/pebbis/storage"
)

// benchStore opens an engine with group-sync so the numbers measure the data
// path and the merge operator, not per-write fsync.
func benchStore(b *testing.B) *Store {
	b.Helper()
	cfg := DefaultOptions()
	cfg.SyncPolicy = config.SyncGroup
	s, err := Open(cfg)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

const benchKey = "counter"

// BenchmarkIncrLockFreeContended is the headline number: many goroutines INCR the
// SAME hot key with no per-key lock. Each increment is a single Merge append; the
// value is folded lazily by the merge operator, so there is no serialisation
// point and no lost update. The final assertion after the run proves correctness.
func BenchmarkIncrLockFreeContended(b *testing.B) {
	s := benchStore(b)
	key := storage.EncodeDataKey(0, benchKey)
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := s.eng.Merge(key, storage.EncodeIntDelta(1)); err != nil {
				b.Fatal(err)
			}
		}
	})
	v, _, err := s.getCounterValue(0, benchKey)
	if err != nil {
		b.Fatal(err)
	}
	if got, _ := strconv.ParseInt(string(v), 10, 64); got != int64(b.N) {
		b.Fatalf("lost updates under lock-free merge: got %d want %d", got, b.N)
	}
}

// BenchmarkIncrLockedRMWContended simulates the pre-merge path that this change
// replaces: a mutex guards a read-modify-write (Get the value, compute +1, Put it
// back) on the same hot key. This is the fairest possible comparison - identical
// engine, identical key, identical read+write - and it MUST be slower because the
// mutex serialises every increment. The delta vs BenchmarkIncrLockFreeContended is
// the payoff of sinking the counter class into the merge operator.
func BenchmarkIncrLockedRMWContended(b *testing.B) {
	s := benchStore(b)
	key := storage.EncodeDataKey(0, benchKey)
	var mu sync.Mutex
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			v, _, err := s.getCounterValue(0, benchKey)
			if err != nil {
				mu.Unlock()
				b.Fatal(err)
			}
			n, _ := strconv.ParseInt(string(v), 10, 64)
			if err := s.eng.Put(key, storage.EncodeValue(config.TypeString, 0, []byte(strconv.FormatInt(n+1, 10)))); err != nil {
				mu.Unlock()
				b.Fatal(err)
			}
			mu.Unlock()
		}
	})
	// Sanity: the locked RMW must also be correct (no lost updates).
	v, _, err := s.getCounterValue(0, benchKey)
	if err != nil {
		b.Fatal(err)
	}
	if got, _ := strconv.ParseInt(string(v), 10, 64); got != int64(b.N) {
		b.Fatalf("lost updates under locked RMW: got %d want %d", got, b.N)
	}
}

// BenchmarkIncrLockFreeDistinctKeys is the no-contention baseline: each iteration
// touches its own key, so even the locked path would not serialise. It isolates
// the per-operation cost of a merge append and proves the lock-free path adds no
// regression when there is nothing to contend on.
func BenchmarkIncrLockFreeDistinctKeys(b *testing.B) {
	s := benchStore(b)
	var seq int64
	b.SetParallelism(8)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := storage.EncodeDataKey(0, "k"+strconv.FormatInt(atomic.AddInt64(&seq, 1), 10))
			if err := s.eng.Merge(k, storage.EncodeIntDelta(1)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestIncrConcurrentNoLostUpdates proves the lock-free guarantee end-to-end at the
// Store level: G goroutines each INCR the same key M times. Because the merge
// operator is associative and commutative, every operand survives and the resolved
// total equals G*M exactly. Run with -race to confirm there is no shared-memory
// hazard (the dispatch layer intentionally does NOT take the per-key lock for
// INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT - see var lockFreeCmds in server.go).
func TestIncrConcurrentNoLostUpdates(t *testing.T) {
	s := testStore(t, nil)
	const G = 32
	const M = 2000
	var wg sync.WaitGroup
	for g := 0; g < G; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < M; i++ {
				if err := s.eng.Merge(storage.EncodeDataKey(0, benchKey), storage.EncodeIntDelta(1)); err != nil {
					t.Errorf("merge: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	v, _, err := s.getCounterValue(0, benchKey)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	want := int64(G * M)
	if got, _ := strconv.ParseInt(string(v), 10, 64); got != want {
		t.Fatalf("expected %d (no lost updates), got %q", want, string(v))
	}
}

// TestIncrFloatConcurrentNoLostUpdates is the float analogue: concurrent
// INCRBYFLOAT-style merges on one key must accumulate to the exact sum. The delta
// 0.5 is exactly representable in binary, so 16000 additions land exactly on 8000.
func TestIncrFloatConcurrentNoLostUpdates(t *testing.T) {
	s := testStore(t, nil)
	const G = 16
	const M = 1000
	var wg sync.WaitGroup
	for g := 0; g < G; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < M; i++ {
				if err := s.eng.Merge(storage.EncodeDataKey(0, "fc"), storage.EncodeFloatDelta("0.5")); err != nil {
					t.Errorf("merge: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	v, _, err := s.getCounterValue(0, "fc")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// G*M * 0.5 = 8000, exact.
	if got := string(v); got != "8000" {
		t.Fatalf("expected 8000, got %q", got)
	}
}
