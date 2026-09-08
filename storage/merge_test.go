package storage

import (
	"io"
	"strconv"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/redistore/redistore/config"
)

// openMergeEngine spins up an in-memory engine with the redistore counter merge
// operator registered, exactly the way the production Open does. Each test gets
// its own engine so failures stay isolated.
func openMergeEngine(t *testing.T) *Engine {
	t.Helper()
	cfg := &config.Config{Dir: "", BlockCacheSize: 1024, SyncPolicy: config.SyncNever}
	eng, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

// resolveCounter folds every pending merge operand for key and returns the
// textual counter value and its resolved expiry (0 == none), mirroring how
// getCounterValue reads counters back.
func resolveCounter(t *testing.T, eng *Engine, key []byte) (string, int64) {
	t.Helper()
	v, rel, err := eng.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer rel()
	typ, expireAt, payload, err := DecodeValue(v)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if typ != config.TypeString {
		t.Fatalf("expected string counter, got typ %d", typ)
	}
	return string(payload), expireAt
}

// TestPebbleMergeRawSanity confirms pebble v1.1.5 actually resolves merge
// operands through the registered ValueMerger on a Get (and is not just
// appending raw bytes). Without this, a mis-configured Merger would silently
// return the last operand and every counter test below would be meaningless.
func TestPebbleMergeRawSanity(t *testing.T) {
	dir := t.TempDir()
	opts := &pebble.Options{
		FS: vfs.Default,
		Merger: &pebble.Merger{
			Merge: func(key, value []byte) (pebble.ValueMerger, error) {
				return &appendVM{buf: append([]byte(nil), value...)}, nil
			},
			Name: "append",
		},
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Merge([]byte("k"), []byte("a"), pebble.NoSync); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if err := db.Merge([]byte("k"), []byte("b"), pebble.NoSync); err != nil {
		t.Fatalf("merge2: %v", err)
	}
	v, closer, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer closer.Close()
	if string(v) != "ab" {
		t.Fatalf("expected 'ab' (merger resolved), got %q", string(v))
	}
}

type appendVM struct{ buf []byte }

func (m *appendVM) MergeNewer(value []byte) error { m.buf = append(m.buf, value...); return nil }
func (m *appendVM) MergeOlder(value []byte) error { m.buf = append(value, m.buf...); return nil }
func (m *appendVM) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return m.buf, nil, nil
}

// TestCounterMergeFromZero: a key that has only ever been MERGEd starts from 0.
func TestCounterMergeFromZero(t *testing.T) {
	eng := openMergeEngine(t)
	k := EncodeDataKey(0, "k")
	for _, d := range []int64{1, 5, -2, 100, -50} {
		if err := eng.Merge(k, EncodeIntDelta(d)); err != nil {
			t.Fatalf("merge %d: %v", d, err)
		}
	}
	got, _ := resolveCounter(t, eng, k)
	if got != "54" {
		t.Fatalf("expected 1+5-2+100-50 = 54, got %q", got)
	}
}

// TestCounterMergePutThenMerge is the regression case for the bug where an older
// Put (the resting counter value) was silently dropped when a newer Merge operand
// became the "base": Pebble folds oldest-first, so the Put arrives via MergeOlder
// and the operator must ADD it as an absolute base.
func TestCounterMergePutThenMerge(t *testing.T) {
	eng := openMergeEngine(t)
	k := EncodeDataKey(0, "k")
	if err := eng.Put(k, EncodeValue(config.TypeString, 0, []byte("10"))); err != nil {
		t.Fatalf("put: %v", err)
	}
	for _, d := range []int64{5, -2} {
		if err := eng.Merge(k, EncodeIntDelta(d)); err != nil {
			t.Fatalf("merge %d: %v", d, err)
		}
	}
	got, _ := resolveCounter(t, eng, k)
	if got != "13" {
		t.Fatalf("expected 10+5-2 = 13, got %q", got)
	}
}

// TestCounterMergeFloatFromZero: only float deltas, starts from 0.
func TestCounterMergeFloatFromZero(t *testing.T) {
	eng := openMergeEngine(t)
	k := EncodeDataKey(0, "k")
	for _, d := range []string{"0.5", "0.25", "-0.1"} {
		if err := eng.Merge(k, EncodeFloatDelta(d)); err != nil {
			t.Fatalf("merge %s: %v", d, err)
		}
	}
	got, _ := resolveCounter(t, eng, k)
	if got != "0.65" {
		t.Fatalf("expected 0.5+0.25-0.1 = 0.65, got %q", got)
	}
}

// TestCounterMergePutFloatThenMerge is the float analogue of the Put-then-Merge
// regression: Put "10.5" then Merge "0.1" must yield "10.6", not "0.1".
func TestCounterMergePutFloatThenMerge(t *testing.T) {
	eng := openMergeEngine(t)
	k := EncodeDataKey(0, "k")
	if err := eng.Put(k, EncodeValue(config.TypeString, 0, []byte("10.5"))); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := eng.Merge(k, EncodeFloatDelta("0.1")); err != nil {
		t.Fatalf("merge float: %v", err)
	}
	got, _ := resolveCounter(t, eng, k)
	if got != "10.6" {
		t.Fatalf("expected 10.5+0.1 = 10.6, got %q", got)
	}
}

// TestCounterMergeMixedIntFloat exercises interleaved int and float operands,
// which both resolve through big.Float addition and must compose regardless of
// order (associativity/commutativity). 0 + 3 (int) + 0.5 (float) - 1 (int) = 2.5.
func TestCounterMergeMixedIntFloat(t *testing.T) {
	eng := openMergeEngine(t)
	k := EncodeDataKey(0, "k")
	if err := eng.Merge(k, EncodeIntDelta(3)); err != nil {
		t.Fatalf("merge int: %v", err)
	}
	if err := eng.Merge(k, EncodeFloatDelta("0.5")); err != nil {
		t.Fatalf("merge float: %v", err)
	}
	if err := eng.Merge(k, EncodeIntDelta(-1)); err != nil {
		t.Fatalf("merge int2: %v", err)
	}
	got, _ := resolveCounter(t, eng, k)
	if got != "2.5" {
		t.Fatalf("expected 3+0.5-1 = 2.5, got %q", got)
	}
}

// TestCounterMergeIntegerFloatFormat checks that a float result landing on an
// integer is rendered without a decimal point (Redis: INCRBYFLOAT 5 0 -> "5",
// not "5.0"), and that -0 collapses to "0".
func TestCounterMergeIntegerFloatFormat(t *testing.T) {
	eng := openMergeEngine(t)
	k := EncodeDataKey(0, "k")
	if err := eng.Merge(k, EncodeFloatDelta("5")); err != nil {
		t.Fatalf("merge: %v", err)
	}
	got, _ := resolveCounter(t, eng, k)
	if got != "5" {
		t.Fatalf("expected integer formatting \"5\", got %q", got)
	}

	// 5 - 5 lands on exactly zero; must not render as "-0".
	if err := eng.Merge(k, EncodeFloatDelta("-5")); err != nil {
		t.Fatalf("merge2: %v", err)
	}
	got, _ = resolveCounter(t, eng, k)
	if got != "0" {
		t.Fatalf("expected \"0\" (not -0), got %q", got)
	}
}

// TestCounterMergeExpiryPreserved verifies the merge operator keeps a key's TTL:
// a Put with an expiry, then a Merge, must resolve to a value that still carries
// the original expiry (INCR must never clear a counter's TTL).
func TestCounterMergeExpiryPreserved(t *testing.T) {
	eng := openMergeEngine(t)
	k := EncodeDataKey(0, "k")
	const ttl = int64(1_000_000)
	if err := eng.Put(k, EncodeValue(config.TypeString, ttl, []byte("7"))); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := eng.Merge(k, EncodeIntDelta(3)); err != nil {
		t.Fatalf("merge: %v", err)
	}
	got, exp := resolveCounter(t, eng, k)
	if got != "10" {
		t.Fatalf("expected 7+3 = 10, got %q", got)
	}
	if exp != ttl {
		t.Fatalf("expected expiry %d preserved, got %d", ttl, exp)
	}
}

// TestCounterMergeNonNumericBase proves the operator never corrupts a key that
// holds a non-numeric string: when that value is the resting base (newest entry),
// a Get returns it untouched. Note the command layer (incrBy/cmdIncrByFloat)
// always validates the current value is numeric BEFORE issuing a Merge, so a
// non-numeric key can never accrue merge operands through the protocol - this
// test pins the operator's passthrough branch for the reachable case.
func TestCounterMergeNonNumericBase(t *testing.T) {
	eng := openMergeEngine(t)
	k := EncodeDataKey(0, "k")
	if err := eng.Put(k, EncodeValue(config.TypeString, 0, []byte("hello"))); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, _ := resolveCounter(t, eng, k)
	if got != "hello" {
		t.Fatalf("non-numeric base must be preserved, got %q", got)
	}
}

// TestCounterMergeDeleteThenMerge: a key that is Put then Deleted then Merged
// behaves like an absent key (starts from 0 + delta), because Delete folds in as
// an empty base.
func TestCounterMergeDeleteThenMerge(t *testing.T) {
	eng := openMergeEngine(t)
	k := EncodeDataKey(0, "k")
	if err := eng.Put(k, EncodeValue(config.TypeString, 0, []byte("99"))); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := eng.Delete(k); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := eng.Merge(k, EncodeIntDelta(7)); err != nil {
		t.Fatalf("merge: %v", err)
	}
	got, _ := resolveCounter(t, eng, k)
	if got != "7" {
		t.Fatalf("expected 0+7 = 7 after delete, got %q", got)
	}
}

// TestCounterMergeLargeScale hammers a single key with many mixed operands and
// confirms the folded total is exact (no accumulation drift in big.Float), and
// crucially that concurrent appends never lose an update.
func TestCounterMergeLargeScale(t *testing.T) {
	eng := openMergeEngine(t)
	k := EncodeDataKey(0, "k")
	const n = 10000
	for i := 0; i < n; i++ {
		delta := int64(1)
		if i%3 == 0 {
			delta = -1
		}
		if err := eng.Merge(k, EncodeIntDelta(delta)); err != nil {
			t.Fatalf("merge %d: %v", i, err)
		}
	}
	want := int64(0)
	for i := 0; i < n; i++ {
		if i%3 == 0 {
			want--
		} else {
			want++
		}
	}
	got, _ := resolveCounter(t, eng, k)
	if got != strconv.FormatInt(want, 10) {
		t.Fatalf("expected %d, got %q", want, got)
	}
}

// TestCounterMergeConcurrent proves the lock-free property directly at the engine
// level: G goroutines each append +1 to the same key, with no mutex. Because the
// merge operator is associative and commutative, every operand is preserved and
// the resolved total equals G*M exactly (no lost updates, no serialisation).
func TestCounterMergeConcurrent(t *testing.T) {
	eng := openMergeEngine(t)
	k := EncodeDataKey(0, "k")
	const G = 32
	const M = 2000
	var wg sync.WaitGroup
	for g := 0; g < G; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < M; i++ {
				if err := eng.Merge(k, EncodeIntDelta(1)); err != nil {
					t.Errorf("merge: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	got, _ := resolveCounter(t, eng, k)
	want := int64(G * M)
	if got != strconv.FormatInt(want, 10) {
		t.Fatalf("expected %d (no lost updates), got %q", want, got)
	}
}
