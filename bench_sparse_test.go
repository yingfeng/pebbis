package redistore

import (
	"strconv"
	"testing"

	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/storage"
)

// The benchmark that justifies the sparse point-write path: with the
// collection already in the sparse representation, a single HSET must cost O(1)
// in the collection size, not O(N).
//
// The store uses group sync so the numbers measure the data path, not fsync.

func openGroupStore(b *testing.B) *Store {
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

func BenchmarkSparseHSet1Field(b *testing.B)    { benchSparseHash(b, 1, 10000) }
func BenchmarkSparseHSet100Fields(b *testing.B) { benchSparseHash(b, 100, 10000) }

// benchSparseHash builds a sparse hash of size fields, then writes batch fields
// to it b.N times.
func benchSparseHash(b *testing.B, batchSize, size int) {
	b.Helper()
	s := openGroupStore(b)

	one := func(i int) [][2][]byte {
		return [][2][]byte{{
			[]byte("f" + strconv.Itoa(i)),
			[]byte("v" + strconv.Itoa(i)),
		}}
	}
	// Build: each call goes through the incremental sparse path, so this is
	// O(size) rather than O(size^2).
	for i := range size {
		if _, err := s.hashSet(0, "bench", one(i), false); err != nil {
			b.Fatalf("hset: %v", err)
		}
	}

	// Confirm it really is in the sparse representation; if this regressed to
	// inline, the benchmark would measure something else entirely.
	if obj, _, exists, err := s.aggEncoding(0, "bench", config.TypeHash); err != nil || !exists {
		b.Fatalf("encoding lookup: exists=%v err=%v", exists, err)
	} else if obj.Enc != storage.EncSparse {
		b.Fatalf("expected sparse encoding, got %d", obj.Enc)
	}

	batch := make([][2][]byte, batchSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range batch {
			idx := (i*batchSize + j) % size
			batch[j] = [2][]byte{
				[]byte("f" + strconv.Itoa(idx)),
				[]byte("v" + strconv.Itoa(idx)),
			}
		}
		if _, err := s.hashSet(0, "bench", batch, false); err != nil {
			b.Fatalf("hset: %v", err)
		}
	}
}

// BenchmarkInlineHSet is the small-collection counterpart, kept for comparison.
func BenchmarkInlineHSet(b *testing.B) {
	s := openGroupStore(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.hashSet(0, "small", [][2][]byte{
			{[]byte("f" + strconv.Itoa(i%50)), []byte("v")},
		}, false); err != nil {
			b.Fatalf("hset: %v", err)
		}
	}
}
