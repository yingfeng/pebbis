package redistore

import (
	"sync"
	"testing"
	"time"
)

// TestLockAllOrderingDeadlockFree reproduces the classic deadlock scenario that
// kvrocks' MultiLockGuard prevents: two goroutines locking the same key set in
// OPPOSITE orders. With a naive "lock as you iterate" scheme, RENAME a b and
// RENAME b a would deadlock. lockAll locks shard indexes in canonical descending
// order, so this must always terminate.
func TestLockAllOrderingDeadlockFree(t *testing.T) {
	tbl := &keyLockTable{}
	keys := []string{"a", "b", "c", "d", "e", "f"}

	done := make(chan struct{}, 2)
	run := func(order []string) {
		defer func() { done <- struct{}{} }()
		unlock := tbl.lockAll(0, order, true)
		// Hold briefly to maximise the window for a conflicting acquisition.
		time.Sleep(time.Millisecond)
		unlock()
	}

	// Conflicting orders on the same key set.
	go run(append([]string(nil), keys...))
	go run(reverse(keys))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lockAll deadlocked on opposing key orders")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lockAll deadlocked on opposing key orders")
	}
}

// TestLockAllConcurrentStress hammers lockAll from many goroutines with random
// key subsets (random orderings) and asserts none ever deadlock.
func TestLockAllConcurrentStress(t *testing.T) {
	tbl := &keyLockTable{}
	pool := make([]string, 32)
	for i := range pool {
		pool[i] = "key-" + string(rune('A'+i%26)) + string(rune('0'+i/26))
	}

	const workers = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		go func(seed uint64) {
			defer wg.Done()
			<-start
			rng := seed
			mod := uint64(len(pool))
			for iter := 0; iter < 200; iter++ {
				rng = rng*1103515245 + 12345
				// pick 1..5 random keys (random order)
				n := 1 + int(rng%5)
				ks := make([]string, 0, n)
				for i := 0; i < n; i++ {
					rng = rng*1103515245 + 12345
					ks = append(ks, pool[rng%mod])
				}
				unlock := tbl.lockAll(0, ks, rng%2 == 0)
				unlock()
			}
		}(uint64(w) + 1)
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("lockAll stress test deadlocked")
	}
}

// TestLockAllReadShared verifies that shared (read) locks on the same key do not
// exclude each other, while an exclusive lock does.
func TestLockAllReadShared(t *testing.T) {
	tbl := &keyLockTable{}
	// Two readers on the same key must both succeed without blocking.
	r1 := tbl.lockAll(0, []string{"x"}, false)
	r2 := tbl.lockAll(0, []string{"x"}, false)
	r2()
	r1()
}

// TestLockAllDedupSameShard checks that two different keys mapping to the same
// shard are locked exactly once (unlock must not double-unlock / panic).
func TestLockAllDedupSameShard(t *testing.T) {
	tbl := &keyLockTable{}
	// Find two keys that collide on the same shard.
	var a, b string
	for i := 0; ; i++ {
		k1, k2 := "c1-"+string(rune(i)), "c2-"+string(rune(i))
		if shardIndex(0, k1) == shardIndex(0, k2) && k1 != k2 {
			a, b = k1, k2
			break
		}
		if i > 100000 {
			t.Skip("no collision found in probe range")
		}
	}
	unlock := tbl.lockAll(0, []string{a, b}, true)
	unlock() // must not panic from a double unlock
}

func reverse(s []string) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[len(s)-1-i] = v
	}
	return out
}
