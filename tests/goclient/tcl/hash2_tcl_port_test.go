package tcl

// Hand-ported tests from frogdb crates/redis-regression/tests/hash_tcl.rs
// (Redis 8.6.0 unit/type/hash.tcl). Previously skip-only stubs emitted by the
// mechanical converter; written by hand against the real Rust source.

import (
	"sort"
	"testing"
)

func Test_TCL_tcl_hgetall_returns_all_fields_and_values(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myhash")
	cDo(t, client, "HSET", "myhash", "a", "1", "b", "2", "c", "3")
	items := extractBulkStrings(t, cDo(t, client, "HGETALL", "myhash"))
	if len(items) != 6 {
		t.Fatalf("expected 6 elements, got %d: %v", len(items), items)
	}
	sort.Strings(items)
	assertStringsEq(t, items, "1", "2", "3", "a", "b", "c")
}
