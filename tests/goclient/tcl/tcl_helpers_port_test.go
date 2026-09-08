package tcl

import (
	"strconv"
	"testing"
)

// assertStringsEqVar compares two string slices element-wise. Go cannot
// compare slices with ==, so `assert_eq!(a, b)` between two slices has to go
// through here.
func assertStringsEqVar(t testing.TB, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("assert_eq! mismatch: got %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("assert_eq! mismatch: got %v want %v", got, want)
		}
	}
}

// intRangeStringsRev is intRangeStrings in descending order, mirroring
// `(start..end).rev().map(|i| i.to_string()).collect()`.
func intRangeStringsRev(start, end int) []string {
	out := make([]string, 0, end-start)
	for i := end - 1; i >= start; i-- {
		out = append(out, strconv.Itoa(i))
	}
	return out
}

// intRangeStrings builds ["start", "start+1", …, "end-1"], mirroring the Rust
// `(start..end).map(|i| i.to_string()).collect()` idiom used by the frogdb
// tests to build expected value lists.
func intRangeStrings(start, end int) []string {
	out := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, strconv.Itoa(i))
	}
	return out
}

// assertExecAborted mirrors frogdb's assert_exec_aborted: an EXEC that was
// aborted by WATCH returns nil/empty.
func assertExecAborted(t testing.TB, r reply) {
	t.Helper()
	switch v := r.val.(type) {
	case nil:
		// nil reply (redis.Nil normalized) is fine
	case []interface{}:
		if len(v) != 0 {
			t.Fatalf("assertExecAborted: expected empty array, got %v", r.val)
		}
	default:
		t.Fatalf("assertExecAborted: expected nil/empty, got %v", r.val)
	}
}

// xreadgroupEntries returns the entries list from a single-stream XREADGROUP
// reply. XREADGROUP replies are [*1 [stream, entries]], so we take the second
// element of the first (and only) stream tuple.
func xreadgroupEntries(t testing.TB, resp reply) []reply {
	t.Helper()
	streams := unwrapArray(t, resp)
	if len(streams) < 1 {
		t.Fatalf("xreadgroupEntries: expected at least one stream, got %v", resp.val)
	}
	tuple := unwrapArray(t, streams[0])
	if len(tuple) < 2 {
		t.Fatalf("xreadgroupEntries: expected [stream, entries], got %v", streams[0])
	}
	return unwrapArray(t, tuple[1])
}

// entryID extracts the entry id (first element) of a [id, [fields...]] entry.
func entryID(t testing.TB, entry reply) string {
	t.Helper()
	arr := unwrapArray(t, entry)
	return parseBulkString(t, arr[0])
}

// entryFields extracts the field values of a [id, [fields...]] entry.
func entryFields(t testing.TB, entry reply) []string {
	t.Helper()
	arr := unwrapArray(t, entry)
	return extractBulkStrings(t, arr[1])
}

// xinfoGetField finds key in a flat alternating key/value array and returns its value.
func xinfoGetField(t testing.TB, items []reply, key string) reply {
	t.Helper()
	for i := 0; i+1 < len(items); i += 2 {
		if parseBulkString(t, items[i]) == key {
			return items[i+1]
		}
	}
	t.Fatalf("xinfoGetField: field %q not found", key)
	return reply{}
}

// findGroupByName finds a group by name within the groups array returned by
// XINFO STREAM FULL and returns that group's field array.
func findGroupByName(t testing.TB, groups []reply, name string) []reply {
	t.Helper()
	for _, g := range groups {
		garr := unwrapArray(t, g)
		if parseBulkString(t, xinfoGetField(t, garr, "name")) == name {
			return garr
		}
	}
	t.Fatalf("findGroupByName: group %q not found", name)
	return nil
}
