package tcl

// Mechanical port of frogdb crates/redis-regression/tests/stream_cgroups_tcl.rs
// (Redis 8.6.0 unit scenarios).
import (
	"fmt"
	"testing"
)
func Test_TCL_tcl_xgroup_create_and_duplicate_group(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "foo", "bar")
	assertOK(t, cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$"))
	// PORT-TODO: );
	resp := cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	assertErrorPrefix(t, resp, "BUSYGROUP")
}

func Test_TCL_tcl_xgroup_create_fails_without_mkstream(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	resp := cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_xgroup_create_works_with_mkstream(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	assertOK(t, cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM"))
	// PORT-TODO: );
}

func Test_TCL_tcl_xreadgroup_basic_argument_count_validation(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	resp := cDoConn(t, client1, "XREADGROUP")
	assertErrorPrefix(t, resp, "ERR")
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP")
	assertErrorPrefix(t, resp, "ERR")
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup")
	assertErrorPrefix(t, resp, "ERR")
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer")
	assertErrorPrefix(t, resp, "ERR")
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer", "STREAMS")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_xreadgroup_group_keyword_validation(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "field", "value")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUPS", "mygroup", "consumer", "STREAMS", "mystream", ">")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_xreadgroup_empty_group_name(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "field", "value")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "", "consumer", "STREAMS", "mystream", ">")
	assertErrorPrefix(t, resp, "NOGROUP")
}

func Test_TCL_tcl_xreadgroup_streams_keyword_validation(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "field", "value")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer", "STREAM", "mystream", ">")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_xreadgroup_stream_and_id_pairing(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "field", "value")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer", "STREAMS", "mystream")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_xreadgroup_count_parameter_validation(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "field", "value")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer", "COUNT", "abc", "STREAMS", "mystream", ">")
	assertErrorPrefix(t, resp, "ERR")
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer", "COUNT", "1.5", "STREAMS", "mystream", ">")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_xreadgroup_block_parameter_validation(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "field", "value")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer", "BLOCK", "abc", "STREAMS", "mystream", ">")
	assertErrorPrefix(t, resp, "ERR")
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer", "BLOCK", "1.5", "STREAMS", "mystream", ">")
	assertErrorPrefix(t, resp, "ERR")
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer", "BLOCK", "-1", "STREAMS", "mystream", ">")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_xreadgroup_stream_id_format_validation(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "field", "value")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	for _, invalid_id := range []string{"invalid-id", "abc-def", "123-abc"} {
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer", "STREAMS", "mystream", invalid_id)
	assertErrorPrefix(t, resp, "ERR")
	}
}

func Test_TCL_tcl_xreadgroup_nonexistent_group(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "field", "value")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "nonexistent", "consumer", "STREAMS", "mystream", ">")
	assertErrorPrefix(t, resp, "NOGROUP")
}

func Test_TCL_tcl_xreadgroup_wrong_key_type(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "wrongtype", "not a stream")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer", "STREAMS", "wrongtype", ">")
	assertErrorPrefix(t, resp, "WRONGTYPE")
}

func Test_TCL_tcl_xreadgroup_returns_only_new_elements(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "foo", "bar")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	cDoConn(t, client1, "XADD", "mystream", "*", "a", "1")
	cDoConn(t, client1, "XADD", "mystream", "*", "b", "2")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-1", "STREAMS", "mystream", ">")
	entries := xreadgroupEntries(t, resp)
	if len(entries) != 2 { t.Fatalf("len mismatch: got %d want 2", len(entries)) }
	fields := entryFields(t, entries[0])
	assertStringsEq(t, fields, "a", "1")
}

func Test_TCL_tcl_xreadgroup_can_read_history(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "foo", "bar")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	cDoConn(t, client1, "XADD", "mystream", "*", "a", "1")
	cDoConn(t, client1, "XADD", "mystream", "*", "b", "2")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-1", "STREAMS", "mystream", ">")
	entries := xreadgroupEntries(t, resp)
	if len(entries) != 2 { t.Fatalf("len mismatch: got %d want 2", len(entries)) }
	cDoConn(t, client1, "XADD", "mystream", "*", "c", "3")
	cDoConn(t, client1, "XADD", "mystream", "*", "d", "4")
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-2", "STREAMS", "mystream", ">")
	entries = xreadgroupEntries(t, resp)
	if len(entries) != 2 { t.Fatalf("len mismatch: got %d want 2", len(entries)) }
	fields := entryFields(t, entries[0])
	assertStringsEq(t, fields, "c", "3")
	r1 := cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-1", "COUNT", "10", "STREAMS", "mystream", "0")
	entries1 := xreadgroupEntries(t, r1)
	fields = entryFields(t, entries1[0])
	assertStringsEq(t, fields, "a", "1")
	r2 := cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-2", "COUNT", "10", "STREAMS", "mystream", "0")
	entries2 := xreadgroupEntries(t, r2)
	fields = entryFields(t, entries2[0])
	assertStringsEq(t, fields, "c", "3")
}

func Test_TCL_tcl_xpending_returns_pending_items(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "foo", "bar")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	cDoConn(t, client1, "XADD", "mystream", "*", "a", "1")
	cDoConn(t, client1, "XADD", "mystream", "*", "b", "2")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-1", "STREAMS", "mystream", ">")
	cDoConn(t, client1, "XADD", "mystream", "*", "c", "3")
	cDoConn(t, client1, "XADD", "mystream", "*", "d", "4")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-2", "STREAMS", "mystream", ">")
	resp := cDoConn(t, client1, "XPENDING", "mystream", "mygroup", "-", "+", "10")
	pending := unwrapArray(t, resp)
	if len(pending) != 4 { t.Fatalf("len mismatch: got %d want 4", len(pending)) }
	for j, entry := range pending {
		if j >= 4 { break }
		item := unwrapArray(t, entry)
		owner := parseBulkString(t, item[1])
		if j < 2 {
			if owner != "consumer-1" { t.Fatalf("assert_eq! mismatch: got %v want consumer-1", owner) }
		} else {
			if owner != "consumer-2" { t.Fatalf("assert_eq! mismatch: got %v want consumer-2", owner) }
		}
	}
}

func Test_TCL_tcl_xpending_single_consumer(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "foo", "bar")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	cDoConn(t, client1, "XADD", "mystream", "*", "a", "1")
	cDoConn(t, client1, "XADD", "mystream", "*", "b", "2")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-1", "STREAMS", "mystream", ">")
	cDoConn(t, client1, "XADD", "mystream", "*", "c", "3")
	cDoConn(t, client1, "XADD", "mystream", "*", "d", "4")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-2", "STREAMS", "mystream", ">")
	resp := cDoConn(t, client1, "XPENDING", "mystream", "mygroup", "-", "+", "10", "consumer-1")
	pending := unwrapArray(t, resp)
	if len(pending) != 2 { t.Fatalf("len mismatch: got %d want 2", len(pending)) }
}

func Test_TCL_tcl_xpending_only_group(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "foo", "bar")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	cDoConn(t, client1, "XADD", "mystream", "*", "a", "1")
	cDoConn(t, client1, "XADD", "mystream", "*", "b", "2")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-1", "STREAMS", "mystream", ">")
	cDoConn(t, client1, "XADD", "mystream", "*", "c", "3")
	cDoConn(t, client1, "XADD", "mystream", "*", "d", "4")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-2", "STREAMS", "mystream", ">")
	resp := cDoConn(t, client1, "XPENDING", "mystream", "mygroup")
	arr := unwrapArray(t, resp)
	if len(arr) != 4 { t.Fatalf("len mismatch: got %d want 4", len(arr)) }
	assertIntegerEq(t, arr[0], 4)
	consumers := unwrapArray(t, arr[3])
	if len(consumers) != 2 { t.Fatalf("len mismatch: got %d want 2", len(consumers)) }
}

func Test_TCL_tcl_xpending_exclusive_range(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "foo", "bar")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	cDoConn(t, client1, "XADD", "mystream", "*", "a", "1")
	cDoConn(t, client1, "XADD", "mystream", "*", "b", "2")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-1", "STREAMS", "mystream", ">")
	cDoConn(t, client1, "XADD", "mystream", "*", "c", "3")
	cDoConn(t, client1, "XADD", "mystream", "*", "d", "4")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-2", "STREAMS", "mystream", ">")
	resp := cDoConn(t, client1, "XPENDING", "mystream", "mygroup", "-", "+", "10")
	pending := unwrapArray(t, resp)
	if len(pending) != 4 { t.Fatalf("len mismatch: got %d want 4", len(pending)) }
	startid := parseBulkString(t, unwrapArray(t, pending[0])[0])
	endid := parseBulkString(t, unwrapArray(t, pending[3])[0])
	start_exclusive := fmt.Sprintf("(%s", startid)
	end_exclusive := fmt.Sprintf("(%s", endid)
	resp = cDoConn(t, client1, "XPENDING", "mystream", "mygroup", start_exclusive, end_exclusive, "10")
	expending := unwrapArray(t, resp)
	if len(expending) != 2 { t.Fatalf("len mismatch: got %d want 2", len(expending)) }
	for _, entry := range expending {
		_ = entry
	item := unwrapArray(t, entry)
	itemid := parseBulkString(t, item[0])
	if itemid == startid { t.Fatalf("assert_ne! mismatch: got %s want != %s", itemid, startid) }
	if itemid == endid { t.Fatalf("assert_ne! mismatch: got %s want != %s", itemid, endid) }
	}
}

func Test_TCL_tcl_xack_removes_from_pel(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "foo", "bar")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$")
	cDoConn(t, client1, "XADD", "mystream", "*", "a", "1")
	cDoConn(t, client1, "XADD", "mystream", "*", "b", "2")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-1", "STREAMS", "mystream", ">")
	cDoConn(t, client1, "XADD", "mystream", "*", "c", "3")
	cDoConn(t, client1, "XADD", "mystream", "*", "d", "4")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-2", "STREAMS", "mystream", ">")
	resp := cDoConn(t, client1, "XPENDING", "mystream", "mygroup", "-", "+", "10", "consumer-1")
	pending := unwrapArray(t, resp)
	id1 := parseBulkString(t, unwrapArray(t, pending[0])[0])
	id2 := parseBulkString(t, unwrapArray(t, pending[1])[0])
	// PORT-TODO: assert_integer_eq(
	cDoConn(t, client1, "XACK", "mystream", "mygroup", id1)
	// PORT-TODO: 1,
	// PORT-TODO: );
	resp = cDoConn(t, client1, "XPENDING", "mystream", "mygroup", "-", "+", "10", "consumer-1")
	pending = unwrapArray(t, resp)
	if len(pending) != 1 { t.Fatalf("len mismatch: got %d want 1", len(pending)) }
	remaining_id := parseBulkString(t, unwrapArray(t, pending[0])[0])
	if remaining_id != id2 { t.Fatalf("assert_eq! mismatch: got %s want %s", remaining_id, id2) }
	resp = cDoConn(t, client1, "XPENDING", "mystream", "mygroup", "-", "+", "10")
	global_pel := unwrapArray(t, resp)
	if len(global_pel) != 3 { t.Fatalf("len mismatch: got %d want 3", len(global_pel)) }
}

func Test_TCL_tcl_xack_no_double_remove(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "a", "1")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "0")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-1", "STREAMS", "mystream", ">")
	resp := cDoConn(t, client1, "XPENDING", "mystream", "mygroup", "-", "+", "10", "consumer-1")
	pending := unwrapArray(t, resp)
	id1 := parseBulkString(t, unwrapArray(t, pending[0])[0])
	// PORT-TODO: assert_integer_eq(
	cDoConn(t, client1, "XACK", "mystream", "mygroup", id1)
	// PORT-TODO: 1,
	// PORT-TODO: );
	// PORT-TODO: assert_integer_eq(
	cDoConn(t, client1, "XACK", "mystream", "mygroup", id1)
	// PORT-TODO: 0,
	// PORT-TODO: );
}

func Test_TCL_tcl_xack_multiple_arguments(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XADD", "mystream", "*", "a", "1")
	cDoConn(t, client1, "XADD", "mystream", "*", "b", "2")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "0")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "consumer-1", "STREAMS", "mystream", ">")
	resp := cDoConn(t, client1, "XPENDING", "mystream", "mygroup", "-", "+", "10", "consumer-1")
	pending := unwrapArray(t, resp)
	id1 := parseBulkString(t, unwrapArray(t, pending[0])[0])
	id2 := parseBulkString(t, unwrapArray(t, pending[1])[0])
	// PORT-TODO: assert_integer_eq(
	cDoConn(t, client1, "XACK", "mystream", "mygroup", id1)
	// PORT-TODO: 1,
	// PORT-TODO: );
	// PORT-TODO: assert_integer_eq(
	// PORT-TODO: &client
	cDoConn(t, client1, "XACK", "mystream", "mygroup", id1, id2)
	// PORT-TODO: 1,
	// PORT-TODO: );
}

func Test_TCL_tcl_xack_fails_on_invalid_id(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "s")
	cDoConn(t, client1, "XGROUP", "CREATE", "s", "g", "$", "MKSTREAM")
	cDoConn(t, client1, "XADD", "s", "*", "f1", "v1")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">")
	entries := xreadgroupEntries(t, resp)
	if len(entries) != 1 { t.Fatalf("len mismatch: got %d want 1", len(entries)) }
	id1 := entryID(t, entries[0])
	resp = cDoConn(t, client1, "XACK", "s", "g", id1, "invalid-id")
	assertErrorPrefix(t, resp, "ERR")
	cDoConn(t, client1, "XACK", "s", "g", id1)
}

func Test_TCL_tcl_pel_nack_reassignment_after_setid(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "events")
	cDoConn(t, client1, "XADD", "events", "*", "f1", "v1")
	cDoConn(t, client1, "XADD", "events", "*", "f1", "v1")
	cDoConn(t, client1, "XADD", "events", "*", "f1", "v1")
	cDoConn(t, client1, "XADD", "events", "*", "f1", "v1")
	cDoConn(t, client1, "XGROUP", "CREATE", "events", "g1", "$")
	cDoConn(t, client1, "XADD", "events", "*", "f1", "v1")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "events", ">")
	entries := xreadgroupEntries(t, resp)
	if len(entries) != 1 { t.Fatalf("len mismatch: got %d want 1", len(entries)) }
	cDoConn(t, client1, "XGROUP", "SETID", "events", "g1", "-")
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP", "g1", "c2", "STREAMS", "events", ">")
	entries = xreadgroupEntries(t, resp)
	if len(entries) != 5 { t.Fatalf("len mismatch: got %d want 5", len(entries)) }
}

func Test_TCL_tcl_xreadgroup_empty_history_bug_5577(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "events")
	cDoConn(t, client1, "XADD", "events", "*", "a", "1")
	cDoConn(t, client1, "XADD", "events", "*", "b", "2")
	cDoConn(t, client1, "XADD", "events", "*", "c", "3")
	cDoConn(t, client1, "XGROUP", "CREATE", "events", "mygroup", "0")
	resp := cDoConn(t, client1, "XPENDING", "events", "mygroup", "-", "+", "10")
	pending := unwrapArray(t, resp)
	if len(pending) != 0 { t.Fatalf("len mismatch: got %d want 0", len(pending)) }
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "myconsumer", "COUNT", "3", "STREAMS", "events", "0")
	streams := unwrapArray(t, resp)
	stream_data := unwrapArray(t, streams[0])
	entries := unwrapArray(t, stream_data[1])
	if len(entries) != 0 { t.Fatalf("len mismatch: got %d want 0", len(entries)) }
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "myconsumer", "COUNT", "3", "STREAMS", "events", ">")
	entries = xreadgroupEntries(t, resp)
	if len(entries) != 3 { t.Fatalf("len mismatch: got %d want 3", len(entries)) }
	resp = cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "myconsumer", "COUNT", "3", "STREAMS", "events", "0")
	entries = xreadgroupEntries(t, resp)
	if len(entries) != 3 { t.Fatalf("len mismatch: got %d want 3", len(entries)) }
}

func Test_TCL_tcl_xreadgroup_deleted_entries_bug_5570(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mystream")
	cDoConn(t, client1, "XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM")
	cDoConn(t, client1, "XADD", "mystream", "1", "field1", "A")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "myconsumer", "STREAMS", "mystream", ">")
	cDoConn(t, client1, "XADD", "mystream", "MAXLEN", "1", "2", "field1", "B")
	cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "myconsumer", "STREAMS", "mystream", ">")
	resp := cDoConn(t, client1, "XREADGROUP", "GROUP", "mygroup", "myconsumer", "STREAMS", "mystream", "0-1")
	entries := xreadgroupEntries(t, resp)
	if len(entries) != 2 { t.Fatalf("len mismatch: got %d want 2", len(entries)) }
	id0 := entryID(t, entries[0])
	if id0 != "1-0" { t.Fatalf("assert_eq! mismatch: got %v want 1-0", id0) }
	fields0 := entryFields(t, entries[0])
	if len(fields0) != 0 { t.Fatalf("len mismatch: got %d want 0", len(fields0)) }
	id1 := entryID(t, entries[1])
	if id1 != "2-0" { t.Fatalf("assert_eq! mismatch: got %v want 2-0", id1) }
	fields1 := entryFields(t, entries[1])
	assertStringsEq(t, fields1, "field1", "B")
}

func Test_TCL_tcl_blocking_xreadgroup_no_empty_array(t *testing.T) {
	t.Skip("TODO: blocking client helper (blocker/writer) not supported by tcl harness")
}
