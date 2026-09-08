package tcl

// Mechanical port of frogdb crates/redis-regression/tests/scan_tcl.rs
// (Redis 8.6.0 unit/scan.tcl scenarios).
//
// Excluded (redistore feature gaps at port time):
//   - All SSCAN/HSCAN/ZSCAN tests: SSCAN/HSCAN/ZSCAN commands not implemented.
//   - tcl_scan_type: SCAN TYPE filtering not implemented (parsed, not applied).
//   - tcl_scan_with_expired_keys: uses DEBUG SET-ACTIVE-EXPIRE (not implemented).

import (
	"sort"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
)

func dedupSorted(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func scanAll(t *testing.T, c *redis.Client, extra ...interface{}) []string {
	t.Helper()
	cursor := "0"
	keys := []string{}
	for {
		args := append([]interface{}{"SCAN", cursor}, extra...)
		r := cDo(t, c, args...)
		arr := unwrapArray(t, r)
		if len(arr) != 2 {
			t.Fatalf("SCAN reply has %d elements, want 2", len(arr))
		}
		cursor = parseBulkString(t, arr[0])
		batch := extractBulkStrings(t, arr[1])
		keys = append(keys, batch...)
		if cursor == "0" {
			break
		}
	}
	return keys
}

func Test_TCL_tcl_scan_basic(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	for i := 0; i < 1000; i++ {
		k := "key:" + strconv.Itoa(i)
		cDo(t, client1, "SET", k, k)
	}
	keys := scanAll(t, client1)
	keys = dedupSorted(keys)
	if len(keys) != 1000 {
		t.Fatalf("got %d keys, want 1000", len(keys))
	}
}

func Test_TCL_tcl_scan_count(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	for i := 0; i < 1000; i++ {
		k := "key:" + strconv.Itoa(i)
		cDo(t, client1, "SET", k, k)
	}
	keys := scanAll(t, client1, "COUNT", "5")
	keys = dedupSorted(keys)
	if len(keys) != 1000 {
		t.Fatalf("got %d keys, want 1000", len(keys))
	}
}

func Test_TCL_tcl_scan_match(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	for i := 0; i < 1000; i++ {
		k := "key:" + strconv.Itoa(i)
		cDo(t, client1, "SET", k, k)
	}
	keys := scanAll(t, client1, "MATCH", "key:1??")
	keys = dedupSorted(keys)
	if len(keys) != 100 {
		t.Fatalf("got %d keys, want 100", len(keys))
	}
}

func Test_TCL_tcl_scan_match_pattern_cluster_slot(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	for j := 0; j < 100; j++ {
		cDo(t, client1, "SET", "{foo}-"+strconv.Itoa(j), "foo")
		cDo(t, client1, "SET", "{bar}-"+strconv.Itoa(j), "bar")
		cDo(t, client1, "SET", "{boo}-"+strconv.Itoa(j), "boo")
	}
	keys := scanAll(t, client1, "MATCH", "{foo}-*")
	keys = dedupSorted(keys)
	if len(keys) != 100 {
		t.Fatalf("got %d keys, want 100", len(keys))
	}
}

func Test_TCL_tcl_scan_guarantees_under_write_load(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	for i := 0; i < 100; i++ {
		k := "key:" + strconv.Itoa(i)
		cDo(t, client1, "SET", k, k)
	}
	cursor := "0"
	keys := []string{}
	added := 0
	for {
		r := cDo(t, client1, "SCAN", cursor)
		arr := unwrapArray(t, r)
		if len(arr) != 2 {
			t.Fatalf("SCAN reply has %d elements, want 2", len(arr))
		}
		cursor = parseBulkString(t, arr[0])
		keys = append(keys, extractBulkStrings(t, arr[1])...)
		if cursor == "0" {
			break
		}
		for i := 0; i < 50; i++ {
			rk := "addedkey:" + strconv.Itoa(added)
			cDo(t, client1, "SET", rk, "foo")
			added++
		}
	}
	if added < 100 {
		t.Fatalf("expected substantial keyspace growth, only added %d", added)
	}
	original := []string{}
	for _, k := range keys {
		if len(k) >= 4 && k[:4] == "key:" {
			original = append(original, k)
		}
	}
	original = dedupSorted(original)
	if len(original) != 100 {
		t.Fatalf("original keys returned %d, want 100", len(original))
	}
}
