package redistore

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/redistore/redistore/config"
)

func testStore(t *testing.T, mutate func(*config.Config)) *Store {
	t.Helper()
	cfg := DefaultOptions()
	cfg.SyncPolicy = config.SyncAlways
	if mutate != nil {
		mutate(cfg)
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSetGet(t *testing.T) {
	s := testStore(t, nil)

	if err := s.Set("k", []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := s.Get("k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("got %q, want %q", got, "v")
	}
}

// TestLargeValueNotInlined exercises the Pebble path: values above
// InlineValueMaxSize are never kept in the dict, so this is the only way they
// are read back.
func TestLargeValueNotInlined(t *testing.T) {
	s := testStore(t, nil)
	big := bytes.Repeat([]byte("x"), 4096)
	if err := s.Set("big", big); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := s.Get("big")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("large value round-trip failed: got %d bytes", len(got))
	}
}

func TestGetMissing(t *testing.T) {
	s := testStore(t, nil)
	got, err := s.Get("nope")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for missing key, got %q", got)
	}
}

func TestOverwrite(t *testing.T) {
	s := testStore(t, nil)
	_ = s.Set("k", []byte("a"))
	_ = s.Set("k", []byte("bbbb"))
	got, _ := s.Get("k")
	if string(got) != "bbbb" {
		t.Fatalf("got %q", got)
	}
}

func TestDelExists(t *testing.T) {
	s := testStore(t, nil)
	_ = s.Set("a", []byte("1"))
	_ = s.Set("b", []byte("2"))

	if n, _ := s.Exists("a", "b", "zz"); n != 2 {
		t.Fatalf("exists = %d, want 2", n)
	}
	if n, _ := s.Del("a", "zz"); n != 1 {
		t.Fatalf("del = %d, want 1", n)
	}
	if got, _ := s.Get("a"); got != nil {
		t.Fatalf("key survived del: %q", got)
	}
}

func TestIncr(t *testing.T) {
	s := testStore(t, nil)
	for want := int64(1); want <= 3; want++ {
		got, err := s.Incr("n")
		if err != nil {
			t.Fatalf("incr: %v", err)
		}
		if got != want {
			t.Fatalf("incr = %d, want %d", got, want)
		}
	}
	if n, err := s.IncrBy("n", -5); err != nil || n != -2 {
		t.Fatalf("incrby = %d, err %v; want -2", n, err)
	}
}

func TestIncrWrongType(t *testing.T) {
	s := testStore(t, nil)
	_ = s.Set("k", []byte("not-a-number"))
	if _, err := s.Incr("k"); err != ErrNotInteger {
		t.Fatalf("err = %v, want ErrNotInteger", err)
	}
}

func TestTTL(t *testing.T) {
	s := testStore(t, nil)
	_ = s.SetEx("k", []byte("v"), 50*time.Millisecond)

	if ttl, ok, exists, _ := s.TTL("k"); !ok || !exists || ttl <= 0 {
		t.Fatalf("ttl = %v ok=%v exists=%v", ttl, ok, exists)
	}
	time.Sleep(80 * time.Millisecond)
	if got, _ := s.Get("k"); got != nil {
		t.Fatalf("expired key still readable: %q", got)
	}
}

func TestPersist(t *testing.T) {
	s := testStore(t, nil)
	_ = s.SetEx("k", []byte("v"), time.Hour)
	if ok, _ := s.Persist("k"); !ok {
		t.Fatal("persist returned false")
	}
	if ttl, ok, _, _ := s.TTL("k"); ok || ttl != 0 {
		t.Fatalf("ttl after persist = %v ok=%v", ttl, ok)
	}
}

func TestMSetMGet(t *testing.T) {
	s := testStore(t, nil)
	if err := s.MSet(map[string][]byte{"a": []byte("1"), "b": []byte("2")}); err != nil {
		t.Fatalf("mset: %v", err)
	}
	got, err := s.MGet("a", "b", "missing")
	if err != nil {
		t.Fatalf("mget: %v", err)
	}
	if string(got[0]) != "1" || string(got[1]) != "2" || got[2] != nil {
		t.Fatalf("mget = %q", got)
	}
}

func TestRename(t *testing.T) {
	s := testStore(t, nil)
	_ = s.Set("src", []byte("v"))
	if err := s.Rename("src", "dst"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if got, _ := s.Get("src"); got != nil {
		t.Fatal("source survived rename")
	}
	if got, _ := s.Get("dst"); string(got) != "v" {
		t.Fatalf("dst = %q", got)
	}
}

func TestAppend(t *testing.T) {
	s := testStore(t, nil)
	if n, _ := s.Append("k", []byte("hello")); n != 5 {
		t.Fatalf("append = %d", n)
	}
	if n, _ := s.Append("k", []byte(" world")); n != 11 {
		t.Fatalf("append = %d", n)
	}
	if got, _ := s.Get("k"); string(got) != "hello world" {
		t.Fatalf("got %q", got)
	}
}

// TestPersistenceRestart is the core durability guarantee: data written before
// a clean shutdown must be readable after reopening the same directory.
func TestPersistenceRestart(t *testing.T) {
	dir := t.TempDir()

	cfg := DefaultOptions()
	cfg.Dir = dir
	cfg.SyncPolicy = config.SyncAlways
	s1, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s1.Set("durable", []byte("yes")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := s1.Set("big", bytes.Repeat([]byte("z"), 8192)); err != nil {
		t.Fatalf("set big: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	cfg2 := DefaultOptions()
	cfg2.Dir = dir
	s2, err := Open(cfg2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if got, _ := s2.Get("durable"); string(got) != "yes" {
		t.Fatalf("after restart, durable = %q", got)
	}
	big, _ := s2.Get("big")
	if len(big) != 8192 {
		t.Fatalf("after restart, big = %d bytes", len(big))
	}
}

// TestEvictionPersistMode checks the central design point: memory stays bounded
// while the data remains readable, because eviction only drops the index entry.
func TestEvictionPersistMode(t *testing.T) {
	s := testStore(t, func(c *config.Config) {
		c.MaxMemory = 4096 // tiny: forces eviction almost immediately
		c.EvictionPolicy = config.AllKeysLRU
		c.EvictionMode = config.EvictPersist
		c.EvictionSample = 5
	})

	const n = 2000
	for i := range n {
		if err := s.Set("key:"+strconv.Itoa(i), []byte("value-"+strconv.Itoa(i))); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	if used := s.UsedMemory(); used > int64(s.cfg.MaxMemory)*2 {
		t.Fatalf("used memory %d greatly exceeds maxmemory %d", used, s.cfg.MaxMemory)
	}
	if s.evictor.Evicted() == 0 {
		t.Fatal("expected some keys to be evicted")
	}

	// The whole point of persist mode: nothing was actually lost.
	for i := range n {
		want := "value-" + strconv.Itoa(i)
		got, err := s.Get("key:" + strconv.Itoa(i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("key %d: got %q, want %q", i, got, want)
		}
	}
	t.Logf("evicted=%d used=%d bytes, all %d keys still served", s.evictor.Evicted(), s.UsedMemory(), n)
}

// TestEvictionCacheMode checks the opposite mode: eviction is destructive.
func TestEvictionCacheMode(t *testing.T) {
	s := testStore(t, func(c *config.Config) {
		c.MaxMemory = 4096
		c.EvictionPolicy = config.AllKeysLRU
		c.EvictionMode = config.EvictCache
	})

	for i := range 2000 {
		_ = s.Set("key:"+strconv.Itoa(i), []byte("value-"+strconv.Itoa(i)))
	}

	var missing int
	for i := range 2000 {
		got, _ := s.Get("key:" + strconv.Itoa(i))
		if got == nil {
			missing++
		}
	}
	if missing == 0 {
		t.Fatal("cache mode should have dropped keys for good")
	}
	t.Logf("cache mode dropped %d/2000 keys", missing)
}

func TestExpiredKeysReaped(t *testing.T) {
	s := testStore(t, func(c *config.Config) {
		c.ExpireCycleInterval = 10 * time.Millisecond
	})
	for i := range 100 {
		_ = s.SetEx("k"+strconv.Itoa(i), []byte("v"), 20*time.Millisecond)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n := s.expirer.Expired(); n > 0 {
			return // the cycle did its job
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("active expiry cycle never removed an expired key")
}

func TestActiveExpiryRemovesFromDisk(t *testing.T) {
	s := testStore(t, func(c *config.Config) {
		c.ExpireCycleInterval = 10 * time.Millisecond
	})
	_ = s.SetEx("gone", []byte("v"), 20*time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.dict.Len(0) == 0 && s.expirer.Expired() > 0 {
			// Also confirm the value is gone from Pebble, not just the index.
			if got, _ := s.Get("gone"); got == nil {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expired key was not removed from storage")
}

func TestDatabaseIsolation(t *testing.T) {
	s := testStore(t, nil)
	_ = s.SetDB(0, "k", []byte("db0"))
	_ = s.SetDB(1, "k", []byte("db1"))

	if got, _ := s.GetDB(0, "k"); string(got) != "db0" {
		t.Fatalf("db0 = %q", got)
	}
	if got, _ := s.GetDB(1, "k"); string(got) != "db1" {
		t.Fatalf("db1 = %q", got)
	}
}

func TestConcurrentSetGet(t *testing.T) {
	s := testStore(t, nil)
	done := make(chan struct{})
	for g := range 8 {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			for i := range 200 {
				key := "k" + strconv.Itoa(g) + "-" + strconv.Itoa(i)
				_ = s.Set(key, []byte("v"))
				if _, err := s.Get(key); err != nil {
					t.Errorf("get %s: %v", key, err)
					return
				}
			}
		}(g)
	}
	for range 8 {
		<-done
	}
}
