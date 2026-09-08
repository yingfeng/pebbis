package tcl

// Hand-ported tests from frogdb crates/redis-regression/tests/stream_tcl.rs
// (Redis 8.6.0 unit/type/stream.tcl). Previously skip-only stubs emitted by
// the mechanical converter; written by hand against the real Rust source.
//
// Deferred (not ported here) — see notes at end of file:
//   - 11 blocking XREAD/XREADGROUP tests (require a 2nd client + timing infra)
//   - 6 XSETID tests (XSETID command is not implemented in redistore)
//   - 5 XADD MAXLEN/MINID/LIMIT option tests (option unsupported; needs a
//     stream-exists marker + node-approximate trimming redistore lacks)
//   - 1 MULTI/EXEC transaction test (go-redis pooled conns break MULTI/EXEC)

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// --- small stream helpers ---------------------------------------------------

// stEntryID returns the entry ID (first element) of an XRANGE/XREAD entry.
func stEntryID(t *testing.T, entry reply) string {
	t.Helper()
	arr := unwrapArray(t, entry)
	return unwrapBulk(t, arr[0])
}

// stEntryFields returns the flat field-value slice of an XRANGE/XREAD entry.
func stEntryFields(t *testing.T, entry reply) []string {
	t.Helper()
	arr := unwrapArray(t, entry)
	return extractBulkStrings(t, arr[1])
}

// stXInfoStreamLength reads the "length" field of an XINFO STREAM reply.
func stXInfoStreamLength(t *testing.T, r reply) int64 {
	t.Helper()
	items := unwrapArray(t, r)
	for i := 0; i+1 < len(items); i += 2 {
		var key string
		switch v := items[i].val.(type) {
		case string:
			key = v
		case []byte:
			key = string(v)
		default:
			continue
		}
		if key == "length" {
			v, ok := items[i+1].val.(int64)
			if !ok {
				t.Fatalf("XINFO STREAM length not an integer: %#v", items[i+1].val)
			}
			return v
		}
	}
	t.Fatalf("XINFO STREAM 'length' field not found")
	return 0
}

// --- XADD + XRANGE basics ----------------------------------------------------

func Test_TCL_tcl_xadd_can_add_entries_and_xrange_can_fetch(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "XADD", "mystream", "*", "item", "1", "value", "a")
	cDo(t, client, "XADD", "mystream", "*", "item", "2", "value", "b")

	assertIntegerEq(t, cDo(t, client, "XLEN", "mystream"), 2)

	entries := unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "-", "+"))
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	f0 := stEntryFields(t, entries[0])
	if f0[0] != "item" || f0[1] != "1" || f0[2] != "value" || f0[3] != "a" {
		t.Fatalf("entry0 fields wrong: %v", f0)
	}
	f1 := stEntryFields(t, entries[1])
	if f1[0] != "item" || f1[1] != "2" || f1[2] != "value" || f1[3] != "b" {
		t.Fatalf("entry1 fields wrong: %v", f1)
	}
}

func Test_TCL_tcl_xadd_ids_are_incremental(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	id1 := unwrapBulk(t, cDo(t, client, "XADD", "mystream", "*", "item", "1", "value", "a"))
	id2 := unwrapBulk(t, cDo(t, client, "XADD", "mystream", "*", "item", "2", "value", "b"))

	ms1, seq1 := parseStreamIDParts(t, id1)
	ms2, seq2 := parseStreamIDParts(t, id2)
	if !(ms2 > ms1 || (ms2 == ms1 && seq2 > seq1)) {
		t.Fatalf("IDs not incremental: %s vs %s", id1, id2)
	}
}

func Test_TCL_tcl_xadd_mass_insertion_and_xlen(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	for i := 0; i < 100; i++ {
		cDo(t, client, "XADD", "mystream", "*", "item", strconv.Itoa(i), "value", fmt.Sprintf("v%d", i))
	}
	assertIntegerEq(t, cDo(t, client, "XLEN", "mystream"), 100)
}

func Test_TCL_tcl_xadd_with_nomkstream_option(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	// NOMKSTREAM on a non-existing stream returns nil and creates nothing.
	r := cDo(t, client, "XADD", "mystream", "NOMKSTREAM", "*", "item", "1")
	assertNil(t, r)
	assertIntegerEq(t, cDo(t, client, "EXISTS", "mystream"), 0)

	// Create the stream, then NOMKSTREAM works.
	cDo(t, client, "XADD", "mystream", "*", "item", "1")
	id := unwrapBulk(t, cDo(t, client, "XADD", "mystream", "NOMKSTREAM", "*", "item", "2"))
	if !strings.Contains(id, "-") {
		t.Fatalf("expected stream ID, got %s", id)
	}
	assertIntegerEq(t, cDo(t, client, "XLEN", "mystream"), 2)
}

func Test_TCL_tcl_xadd_advances_entries_added_counter(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "XADD", "mystream", "*", "a", "1")
	cDo(t, client, "XADD", "mystream", "*", "b", "2")
	cDo(t, client, "XADD", "mystream", "*", "c", "3")

	assertIntegerEq(t, reply{val: stXInfoStreamLength(t, cDo(t, client, "XINFO", "STREAM", "mystream"))}, 3)
}

func Test_TCL_tcl_xadd_stream_id_edge(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	assertBulkEq(t, cDo(t, client, "XADD", "mystream", "0-1", "a", "1"), "0-1")
	assertBulkEq(t, cDo(t, client, "XADD", "mystream", "0-2", "b", "2"), "0-2")

	entries := unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "-", "+"))
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	assertBulkEq(t, unwrapArray(t, entries[0])[0], "0-1")
	assertBulkEq(t, unwrapArray(t, entries[1])[0], "0-2")
}

// --- XADD partial auto-sequence ("ms-*") and 0-0 ----------------------------

func Test_TCL_tcl_xadd_auto_seq_incremented_for_last_id(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "XADD", "mystream", "123-456", "k", "v")
	id := unwrapBulk(t, cDo(t, client, "XADD", "mystream", "123-*", "k", "v"))
	assertBulkEq(t, reply{val: id}, "123-457")
}

func Test_TCL_tcl_xadd_auto_seq_zero_for_future_timestamp(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "XADD", "mystream", "123-456", "k", "v")
	id := unwrapBulk(t, cDo(t, client, "XADD", "mystream", "789-*", "k", "v"))
	assertBulkEq(t, reply{val: id}, "789-0")
}

func Test_TCL_tcl_xadd_zero_star_should_succeed(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	id := unwrapBulk(t, cDo(t, client, "XADD", "mystream", "0-*", "k", "v"))
	// 0-* on an empty stream yields 0-1 (since 0-0 is the minimum).
	assertBulkEq(t, reply{val: id}, "0-1")
}

func Test_TCL_tcl_xadd_with_id_zero_zero(t *testing.T) {
	// frogdb deviates from Redis here: real Redis rejects XADD "0-0"
	// ("The ID specified in XADD must be greater than 0-0"), which redistore
	// and the compat suite assert. This frogdb expectation cannot hold.
	t.Skip("frogdb expects XADD 0-0 to succeed, but Redis rejects it (redistore matches Redis)")
	addr := startServer(t)
	client := connect(t, addr)

	// Redis allows 0-0 on an empty stream.
	id := unwrapBulk(t, cDo(t, client, "XADD", "mystream", "0-0", "k", "v"))
	assertBulkEq(t, reply{val: id}, "0-0")

	// Adding another 0-0 must be rejected (not monotonic).
	assertErrorPrefix(t, cDo(t, client, "XADD", "mystream", "0-0", "k", "v"), "ERR")
}

func Test_TCL_tcl_xadd_partial_id_with_maximal_seq(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	id := unwrapBulk(t, cDo(t, client, "XADD", "mystream", "1-0", "k", "v"))
	assertBulkEq(t, reply{val: id}, "1-0")

	id = unwrapBulk(t, cDo(t, client, "XADD", "mystream", "1-*", "k", "v2"))
	assertBulkEq(t, reply{val: id}, "1-1")

	cDo(t, client, "XADD", "mystream", "1-18446744073709551615", "k", "v3")

	// Auto-seq at ms=1 now fails: the sequence is at its maximum.
	assertErrorPrefix(t, cDo(t, client, "XADD", "mystream", "1-*", "k", "v4"), "ERR")
}

// --- XRANGE / XREVRANGE -----------------------------------------------------

func Test_TCL_tcl_xrange_exclusive_ranges(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	for i := 1; i <= 5; i++ {
		cDo(t, client, "XADD", "mystream", strconv.Itoa(i)+"-0", "k", "v"+strconv.Itoa(i))
	}

	// Exclusive start: (1-0 means start after 1-0.
	entries := unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "(1-0", "5-0"))
	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}
	assertBulkEq(t, unwrapArray(t, entries[0])[0], "2-0")

	// Exclusive end: (5-0 means end before 5-0.
	entries = unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "1-0", "(5-0"))
	if len(entries) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(entries))
	}
	assertBulkEq(t, unwrapArray(t, entries[3])[0], "4-0")

	// Both exclusive.
	entries = unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "(1-0", "(5-0"))
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	assertBulkEq(t, unwrapArray(t, entries[0])[0], "2-0")
	assertBulkEq(t, unwrapArray(t, entries[2])[0], "4-0")
}

func Test_TCL_tcl_xrevrange_regression_issue_5006(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	for i := 0; i < 100; i++ {
		cDo(t, client, "XADD", "mystream", "*", "item", strconv.Itoa(i))
	}

	entries := unwrapArray(t, cDo(t, client, "XREVRANGE", "mystream", "+", "-", "COUNT", "1"))
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	f := stEntryFields(t, entries[0])
	if f[0] != "item" || f[1] != "99" {
		t.Fatalf("expected item 99, got %v", f)
	}
}

func Test_TCL_tcl_xrange_can_be_used_to_iterate_whole_stream(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	const total = 500
	for j := 0; j < total; j++ {
		cDo(t, client, "XADD", "mystream", "*", "item", strconv.Itoa(j))
	}

	j := 0
	lastID := "-"
	for {
		entries := unwrapArray(t, cDo(t, client, "XRANGE", "mystream", lastID, "+", "COUNT", "100"))
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			f := stEntryFields(t, e)
			if f[0] != "item" || f[1] != strconv.Itoa(j) {
				t.Fatalf("entry %d mismatch: %v", j, f)
			}
			j++
		}
		last := stEntryID(t, entries[len(entries)-1])
		ms, seq := parseStreamIDParts(t, last)
		lastID = fmt.Sprintf("%d-%d", ms, seq+1)
	}
	if j != total {
		t.Fatalf("iterated %d entries, expected %d", j, total)
	}
}

func Test_TCL_tcl_xrevrange_returns_reverse_of_xrange(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	for j := 0; j < 50; j++ {
		cDo(t, client, "XADD", "mystream", "*", "item", strconv.Itoa(j))
	}

	forward := unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "-", "+"))
	reverse := unwrapArray(t, cDo(t, client, "XREVRANGE", "mystream", "+", "-"))
	if len(forward) != len(reverse) {
		t.Fatalf("length mismatch: %d vs %d", len(forward), len(reverse))
	}
	last := len(forward) - 1
	for i, fwd := range forward {
		rev := reverse[last-i]
		if stEntryID(t, fwd) != stEntryID(t, rev) {
			t.Fatalf("ID mismatch at %d", i)
		}
		fa := stEntryFields(t, fwd)
		ra := stEntryFields(t, rev)
		if fa[0] != ra[0] || fa[1] != ra[1] {
			t.Fatalf("fields mismatch at %d: %v vs %v", i, fa, ra)
		}
	}
}

// --- XDEL -------------------------------------------------------------------

func Test_TCL_tcl_xdel_basic(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "XADD", "mystream", "1-0", "a", "1")
	cDo(t, client, "XADD", "mystream", "2-0", "b", "2")
	cDo(t, client, "XADD", "mystream", "3-0", "c", "3")

	assertIntegerEq(t, cDo(t, client, "XDEL", "mystream", "2-0"), 1)

	entries := unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "-", "+"))
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	assertBulkEq(t, unwrapArray(t, entries[0])[0], "1-0")
	assertBulkEq(t, unwrapArray(t, entries[1])[0], "3-0")
}

func Test_TCL_tcl_xdel_multiple_ids(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "XADD", "mystream", "1-0", "a", "1")
	cDo(t, client, "XADD", "mystream", "2-0", "b", "2")
	cDo(t, client, "XADD", "mystream", "3-0", "c", "3")
	cDo(t, client, "XADD", "mystream", "4-0", "d", "4")

	assertIntegerEq(t, cDo(t, client, "XDEL", "mystream", "2-0", "3-0"), 2)
	assertIntegerEq(t, cDo(t, client, "XLEN", "mystream"), 2)

	entries := unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "-", "+"))
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	assertBulkEq(t, unwrapArray(t, entries[0])[0], "1-0")
	assertBulkEq(t, unwrapArray(t, entries[1])[0], "4-0")
}

func Test_TCL_tcl_xdel_multiply_id_test(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "XADD", "somestream", "1-1", "a", "1")
	cDo(t, client, "XADD", "somestream", "1-2", "b", "2")
	cDo(t, client, "XADD", "somestream", "1-3", "c", "3")
	cDo(t, client, "XADD", "somestream", "1-4", "d", "4")
	cDo(t, client, "XADD", "somestream", "1-5", "e", "5")

	assertIntegerEq(t, cDo(t, client, "XLEN", "somestream"), 5)
	assertIntegerEq(t, cDo(t, client, "XDEL", "somestream", "1-1", "1-4", "1-5", "2-1"), 3)
	assertIntegerEq(t, cDo(t, client, "XLEN", "somestream"), 2)

	entries := unwrapArray(t, cDo(t, client, "XRANGE", "somestream", "-", "+"))
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	assertBulkEq(t, unwrapArray(t, entries[0])[0], "1-2")
	assertBulkEq(t, unwrapArray(t, entries[1])[0], "1-3")
}

func Test_TCL_tcl_maximum_xdel_id_behaves_correctly(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "XADD", "mystream", "1-0", "a", "1")
	cDo(t, client, "XADD", "mystream", "2-0", "b", "2")
	cDo(t, client, "XADD", "mystream", "3-0", "c", "3")

	// Non-existing entry returns 0.
	assertIntegerEq(t, cDo(t, client, "XDEL", "mystream", "999-0"), 0)
	// Existing entry.
	assertIntegerEq(t, cDo(t, client, "XDEL", "mystream", "2-0"), 1)
	// Can still add after deletion.
	cDo(t, client, "XADD", "mystream", "4-0", "d", "4")
	assertIntegerEq(t, cDo(t, client, "XLEN", "mystream"), 3)
}

func Test_TCL_tcl_xdel_trim_reflected_by_first_entry(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "XADD", "mystream", "1-0", "a", "1")
	cDo(t, client, "XADD", "mystream", "2-0", "b", "2")
	cDo(t, client, "XADD", "mystream", "3-0", "c", "3")

	assertIntegerEq(t, cDo(t, client, "XDEL", "mystream", "1-0"), 1)

	entries := unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "-", "+"))
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	assertBulkEq(t, unwrapArray(t, entries[0])[0], "2-0")

	trimmed := unwrapToInt(t, cDo(t, client, "XTRIM", "mystream", "MAXLEN", "1"))
	if trimmed != 1 {
		t.Fatalf("expected 1 trimmed, got %d", trimmed)
	}
	entries = unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "-", "+"))
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	assertBulkEq(t, unwrapArray(t, entries[0])[0], "3-0")
}

// --- XTRIM ------------------------------------------------------------------

func Test_TCL_tcl_xtrim_with_minid_option(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	for i := 1; i <= 10; i++ {
		cDo(t, client, "XADD", "mystream", strconv.Itoa(i)+"-0", "k", "v"+strconv.Itoa(i))
	}
	trimmed := unwrapToInt(t, cDo(t, client, "XTRIM", "mystream", "MINID", "5"))
	if trimmed != 4 {
		t.Fatalf("expected 4 trimmed, got %d", trimmed)
	}
	assertIntegerEq(t, cDo(t, client, "XLEN", "mystream"), 6)

	entries := unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "-", "+"))
	for _, e := range entries {
		ms, _ := parseStreamIDParts(t, stEntryID(t, e))
		if ms < 3 {
			t.Fatalf("entry %s should have been trimmed", stEntryID(t, e))
		}
	}
}

func Test_TCL_tcl_xtrim_with_minid_big_delta(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	for i := 1; i <= 10; i++ {
		cDo(t, client, "XADD", "mystream", strconv.Itoa(i)+"-0", "k", "v"+strconv.Itoa(i))
	}
	trimmed := unwrapToInt(t, cDo(t, client, "XTRIM", "mystream", "MINID", "8"))
	if trimmed != 7 {
		t.Fatalf("expected 7 trimmed, got %d", trimmed)
	}
	assertIntegerEq(t, cDo(t, client, "XLEN", "mystream"), 3)

	entries := unwrapArray(t, cDo(t, client, "XRANGE", "mystream", "-", "+"))
	assertBulkEq(t, unwrapArray(t, entries[0])[0], "8-0")
}

func Test_TCL_tcl_xtrim_with_maxlen_basic(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	for i := 0; i < 100; i++ {
		cDo(t, client, "XADD", "mystream", "*", "item", strconv.Itoa(i))
	}
	trimmed := unwrapToInt(t, cDo(t, client, "XTRIM", "mystream", "MAXLEN", "10"))
	if trimmed != 90 {
		t.Fatalf("expected 90 trimmed, got %d", trimmed)
	}
	assertIntegerEq(t, cDo(t, client, "XLEN", "mystream"), 10)
}

func Test_TCL_tcl_xtrim_without_approx_with_limit(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	for i := 0; i < 100; i++ {
		cDo(t, client, "XADD", "mystream", "*", "item", strconv.Itoa(i))
	}
	trimmed := unwrapToInt(t, cDo(t, client, "XTRIM", "mystream", "MAXLEN", "~", "0", "LIMIT", "30"))
	if trimmed <= 0 {
		t.Fatalf("expected some entries trimmed, got %d", trimmed)
	}
}

func Test_TCL_tcl_xtrim_with_approximate_is_limited(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	for i := 0; i < 102; i++ {
		cDo(t, client, "XADD", "mystream", "*", "xitem", "v")
	}
	trimmed := unwrapToInt(t, cDo(t, client, "XTRIM", "mystream", "MAXLEN", "~", "1"))
	if trimmed <= 0 {
		t.Fatalf("expected some entries trimmed, got %d", trimmed)
	}
	remaining := unwrapToInt(t, cDo(t, client, "XLEN", "mystream"))
	if remaining < 1 || remaining > 2 {
		t.Fatalf("expected 1 or 2 remaining entries, got %d", remaining)
	}
}

// --- XREAD (non-blocking) ---------------------------------------------------

func Test_TCL_tcl_xread_with_non_empty_stream(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "XADD", "s1", "*", "k", "v1")
	cDo(t, client, "XADD", "s1", "*", "k", "v2")

	streams := unwrapArray(t, cDo(t, client, "XREAD", "COUNT", "10", "STREAMS", "s1", "0-0"))
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	streamData := unwrapArray(t, streams[0])
	assertBulkEq(t, streamData[0], "s1")
	entries := unwrapArray(t, streamData[1])
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func Test_TCL_tcl_xread_last_element_after_xdel(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	id1 := unwrapBulk(t, cDo(t, client, "XADD", "s1", "*", "a", "1"))
	cDo(t, client, "XADD", "s1", "*", "b", "2")
	id3 := unwrapBulk(t, cDo(t, client, "XADD", "s1", "*", "c", "3"))

	assertIntegerEq(t, cDo(t, client, "XDEL", "s1", id1), 1)

	streams := unwrapArray(t, cDo(t, client, "XREAD", "STREAMS", "s1", "0-0"))
	streamData := unwrapArray(t, streams[0])
	entries := unwrapArray(t, streamData[1])
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// Reading from the last entry ID returns nil (no new entries).
	r := cDo(t, client, "XREAD", "STREAMS", "s1", id3)
	assertNil(t, r)
}

func Test_TCL_tcl_xread_stream_id_edge_non_blocking(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "XADD", "s1", "1-1", "a", "1")
	cDo(t, client, "XADD", "s1", "1-2", "b", "2")
	cDo(t, client, "XADD", "s1", "1-3", "c", "3")

	// Read from 1-1 -> 1-2 and 1-3.
	streams := unwrapArray(t, cDo(t, client, "XREAD", "STREAMS", "s1", "1-1"))
	streamData := unwrapArray(t, streams[0])
	entries := unwrapArray(t, streamData[1])
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	assertBulkEq(t, unwrapArray(t, entries[0])[0], "1-2")
	assertBulkEq(t, unwrapArray(t, entries[1])[0], "1-3")

	// Read from 1-3 -> nil.
	r := cDo(t, client, "XREAD", "STREAMS", "s1", "1-3")
	assertNil(t, r)
}

// --- XGROUP / XINFO HELP ----------------------------------------------------

func Test_TCL_tcl_xgroup_help_should_not_have_unexpected_options(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	r := cDo(t, client, "XGROUP", "HELP", "xxx")
	if r.err != nil {
		// Redis-compatible rejection of extra args is acceptable.
		if !strings.HasPrefix(r.err.Error(), "ERR") {
			t.Fatalf("unexpected error: %v", r.err)
		}
		return
	}
	items := extractBulkStrings(t, r)
	if len(items) == 0 {
		t.Fatalf("XGROUP HELP returned empty array")
	}
	found := false
	for _, s := range items {
		if strings.Contains(s, "XGROUP") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("XGROUP HELP missing 'XGROUP' keyword: %v", items)
	}
}

func Test_TCL_tcl_xinfo_help_should_not_have_unexpected_options(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	r := cDo(t, client, "XINFO", "HELP", "xxx")
	if r.err != nil {
		if !strings.HasPrefix(r.err.Error(), "ERR") {
			t.Fatalf("unexpected error: %v", r.err)
		}
		return
	}
	items := extractBulkStrings(t, r)
	if len(items) == 0 {
		t.Fatalf("XINFO HELP returned empty array")
	}
	found := false
	for _, s := range items {
		if strings.Contains(s, "XINFO") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("XINFO HELP missing 'XINFO' keyword: %v", items)
	}
}

// --- helpers ----------------------------------------------------------------

// parseStreamIDParts parses "ms-seq" into its two components.
func parseStreamIDParts(t *testing.T, id string) (uint64, uint64) {
	t.Helper()
	parts := strings.SplitN(id, "-", 2)
	if len(parts) != 2 {
		t.Fatalf("malformed stream id %q", id)
	}
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		t.Fatalf("bad ms in %q: %v", id, err)
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		t.Fatalf("bad seq in %q: %v", id, err)
	}
	return ms, seq
}

// unwrapToInt decodes an integer reply value.
func unwrapToInt(t *testing.T, r reply) int64 {
	t.Helper()
	if r.err != nil {
		t.Fatalf("unexpected error: %v", r.err)
	}
	v, ok := r.val.(int64)
	if !ok {
		t.Fatalf("expected int reply, got %#v", r.val)
	}
	return v
}
