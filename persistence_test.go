package pebbis

import (
	"strconv"
	"testing"

	"github.com/pebbis/pebbis/config"
	"github.com/pebbis/pebbis/storage"
)

// Restart persistence for the aggregate types.
//
// This is the check that matters most for the sparse representation: elements
// live in separate keys from the header, so a bug that writes one and not the
// other would survive every in-process assertion and only show up after a
// restart. It is done in-process rather than by shelling out to the binary,
// which was both slow and prone to leaving a locked data directory behind.

func openAt(t *testing.T, dir string) *Store {
	t.Helper()
	cfg := DefaultOptions()
	cfg.Dir = dir
	cfg.SyncPolicy = config.SyncAlways
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	return s
}

func TestAggregateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// N is well past InlineMaxEntries, so these are stored sparsely.
	const n = 300

	s1 := openAt(t, dir)
	for i := range n {
		if _, err := s1.hashSet(0, "h", [][2][]byte{
			{[]byte("f" + strconv.Itoa(i)), []byte("v" + strconv.Itoa(i))},
		}, false); err != nil {
			t.Fatalf("hset %d: %v", i, err)
		}
	}
	for i := range n {
		if _, err := s1.listPush(0, "l", []string{strconv.Itoa(i)}, false, false); err != nil {
			t.Fatalf("rpush %d: %v", i, err)
		}
	}
	for i := range n {
		if _, err := s1.setAdd(0, "s", []string{"m" + strconv.Itoa(i)}); err != nil {
			t.Fatalf("sadd %d: %v", i, err)
		}
	}
	for i := range n {
		if _, _, err := s1.zAdd(0, "z", []storage.Member{{
			Member: "m" + strconv.Itoa(i), Score: float64(i),
		}}, ZAddOption{}); err != nil {
			t.Fatalf("zadd %d: %v", i, err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := openAt(t, dir)
	defer func() { _ = s2.Close() }()

	// Hash: header plus every element key must have come back.
	h, err := s2.loadAgg(0, "h", config.TypeHash)
	if err != nil {
		t.Fatalf("load hash: %v", err)
	}
	if len(h.hash) != n {
		t.Errorf("hash: %d fields after restart, want %d", len(h.hash), n)
	}
	if string(h.hash["f0"]) != "v0" || string(h.hash["f"+strconv.Itoa(n-1)]) != "v"+strconv.Itoa(n-1) {
		t.Error("hash: element values did not survive the restart")
	}

	// List: order matters, and it is the thing sparse sequence numbers exist
	// to preserve.
	l, err := s2.loadAgg(0, "l", config.TypeList)
	if err != nil {
		t.Fatalf("load list: %v", err)
	}
	if len(l.list) != n {
		t.Fatalf("list: %d elements after restart, want %d", len(l.list), n)
	}
	if l.list[0] != "0" || l.list[n-1] != strconv.Itoa(n-1) {
		t.Errorf("list: order not preserved, got first=%q last=%q", l.list[0], l.list[n-1])
	}

	// Set.
	s, err := s2.loadAgg(0, "s", config.TypeSet)
	if err != nil {
		t.Fatalf("load set: %v", err)
	}
	if len(s.set) != n {
		t.Errorf("set: %d members after restart, want %d", len(s.set), n)
	}
	if _, ok := s.set["m"+strconv.Itoa(n-1)]; !ok {
		t.Error("set: last member missing after restart")
	}

	// Sorted set: both the member->score and score index must be intact.
	z, err := s2.loadAgg(0, "z", config.TypeZSet)
	if err != nil {
		t.Fatalf("load zset: %v", err)
	}
	if len(z.zset) != n {
		t.Errorf("zset: %d members after restart, want %d", len(z.zset), n)
	}
	if z.zset["m0"] != 0 || z.zset["m"+strconv.Itoa(n-1)] != float64(n-1) {
		t.Error("zset: scores did not survive the restart")
	}
}

// TestDeleteSurvivesRestart guards the counterpart: elements deleted before a
// restart must not reappear.
func TestDeleteSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	s1 := openAt(t, dir)
	for i := range 300 {
		if _, err := s1.hashSet(0, "h", [][2][]byte{
			{[]byte("f" + strconv.Itoa(i)), []byte("v")},
		}, false); err != nil {
			t.Fatalf("hset: %v", err)
		}
	}
	if _, err := s1.hashDel(0, "h", []string{"f0", "f1", "f2"}); err != nil {
		t.Fatalf("hdel: %v", err)
	}
	if _, err := s1.deleteKey(0, "h"); err != nil {
		t.Fatalf("del: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2 := openAt(t, dir)
	defer func() { _ = s2.Close() }()

	if got, err := s2.exists(0, []string{"h"}); err != nil || got != 0 {
		t.Errorf("deleted key reappeared after restart: exists=%d err=%v", got, err)
	}
	// Recreating it must start clean, with no residue from the old elements.
	if _, err := s2.hashSet(0, "h", [][2][]byte{{[]byte("only"), []byte("1")}}, false); err != nil {
		t.Fatalf("hset: %v", err)
	}
	h, err := s2.loadAgg(0, "h", config.TypeHash)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(h.hash) != 1 {
		t.Errorf("recreated hash has %d fields, want 1 - old elements leaked", len(h.hash))
	}
}
