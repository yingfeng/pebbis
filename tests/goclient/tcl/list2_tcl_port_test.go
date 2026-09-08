package tcl

// Hand-ported tests from frogdb crates/redis-regression/tests/list_tcl.rs
// (Redis 8.6.0 unit/type/list.tcl). Previously skip-only stubs emitted by the
// mechanical converter; written by hand against the real Rust source.

import (
	"strconv"
	"testing"
)

func Test_TCL_tcl_lpos_count_option(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mylist")
	cDo(t, client, "RPUSH", "mylist", "a", "b", "c", "d", "2", "3", "c", "c")
	items := unwrapArray(t, cDo(t, client, "LPOS", "mylist", "c", "COUNT", "0"))
	if len(items) != 3 {
		t.Fatalf("expected 3 matches, got %d: %v", len(items), items)
	}
	assertIntegerEq(t, items[0], 2)
	assertIntegerEq(t, items[1], 6)
	assertIntegerEq(t, items[2], 7)

	items = unwrapArray(t, cDo(t, client, "LPOS", "mylist", "c", "COUNT", "1"))
	assertIntegerEq(t, items[0], 2)

	items = unwrapArray(t, cDo(t, client, "LPOS", "mylist", "c", "COUNT", "2"))
	assertIntegerEq(t, items[0], 2)
	assertIntegerEq(t, items[1], 6)
}

func Test_TCL_tcl_lpos_count_plus_rank(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mylist")
	cDo(t, client, "RPUSH", "mylist", "a", "b", "c", "d", "2", "3", "c", "c")
	items := unwrapArray(t, cDo(t, client, "LPOS", "mylist", "c", "COUNT", "0", "RANK", "2"))
	assertIntegerEq(t, items[0], 6)
	assertIntegerEq(t, items[1], 7)

	items = unwrapArray(t, cDo(t, client, "LPOS", "mylist", "c", "COUNT", "2", "RANK", "-1"))
	assertIntegerEq(t, items[0], 7)
	assertIntegerEq(t, items[1], 6)
}

func Test_TCL_tcl_lpos_maxlen(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mylist")
	cDo(t, client, "RPUSH", "mylist", "a", "b", "c", "d", "2", "3", "c", "c")

	items := unwrapArray(t, cDo(t, client, "LPOS", "mylist", "a", "COUNT", "0", "MAXLEN", "1"))
	assertIntegerEq(t, items[0], 0)

	items = unwrapArray(t, cDo(t, client, "LPOS", "mylist", "c", "COUNT", "0", "MAXLEN", "1"))
	if len(items) != 0 {
		t.Fatalf("expected no matches, got %d: %v", len(items), items)
	}

	items = unwrapArray(t, cDo(t, client, "LPOS", "mylist", "c", "COUNT", "0", "MAXLEN", "3"))
	assertIntegerEq(t, items[0], 2)
}

func Test_TCL_tcl_rpop_lpop_with_optional_count_argument(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "listcount")
	assertIntegerEq(t, cDo(t, client, "LPUSH", "listcount", "aa", "bb", "cc", "dd", "ee", "ff", "gg"), 7)

	items := unwrapArray(t, cDo(t, client, "LPOP", "listcount", "1"))
	assertBulkEq(t, items[0], "gg")
	items = unwrapArray(t, cDo(t, client, "LPOP", "listcount", "2"))
	assertBulkEq(t, items[0], "ff")
	assertBulkEq(t, items[1], "ee")
	items = unwrapArray(t, cDo(t, client, "RPOP", "listcount", "2"))
	assertBulkEq(t, items[0], "aa")
	assertBulkEq(t, items[1], "bb")
	items = unwrapArray(t, cDo(t, client, "RPOP", "listcount", "1"))
	assertBulkEq(t, items[0], "cc")
	items = unwrapArray(t, cDo(t, client, "RPOP", "listcount", "123"))
	assertBulkEq(t, items[0], "dd")

	// Negative count
	assertErrorPrefix(t, cDo(t, client, "LPOP", "forbarqaz", "-123"), "ERR")
}

func Test_TCL_tcl_variadic_rpush_lpush(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mylist")
	assertIntegerEq(t, cDo(t, client, "LPUSH", "mylist", "a", "b", "c", "d"), 4)
	assertIntegerEq(t, cDo(t, client, "RPUSH", "mylist", "0", "1", "2", "3"), 8)
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist", "0", "-1")), "d", "c", "b", "a", "0", "1", "2", "3")
}

func Test_TCL_tcl_lpushx_rpushx(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "xlist")
	cDo(t, client, "RPUSH", "xlist", "a", "c")
	assertIntegerEq(t, cDo(t, client, "RPUSHX", "xlist", "d"), 3)
	assertIntegerEq(t, cDo(t, client, "LPUSHX", "xlist", "z"), 4)
	assertIntegerEq(t, cDo(t, client, "RPUSHX", "xlist", "42", "x"), 6)
	assertIntegerEq(t, cDo(t, client, "LPUSHX", "xlist", "y3", "y2", "y1"), 9)
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "xlist", "0", "-1")), "y1", "y2", "y3", "z", "a", "c", "d", "42", "x")
}

func Test_TCL_tcl_rpoplpush_base_case(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mylist1{t}", "mylist2{t}")
	cDo(t, client, "RPUSH", "mylist1{t}", "a", "b", "c", "d")
	assertBulkEq(t, cDo(t, client, "RPOPLPUSH", "mylist1{t}", "mylist2{t}"), "d")
	assertBulkEq(t, cDo(t, client, "RPOPLPUSH", "mylist1{t}", "mylist2{t}"), "c")
	assertBulkEq(t, cDo(t, client, "RPOPLPUSH", "mylist1{t}", "mylist2{t}"), "b")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist1{t}", "0", "-1")), "a")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist2{t}", "0", "-1")), "b", "c", "d")
}

func Test_TCL_tcl_rpoplpush_with_same_list_as_src_and_dst(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mylist{t}")
	cDo(t, client, "RPUSH", "mylist{t}", "a", "b", "c")
	assertBulkEq(t, cDo(t, client, "RPOPLPUSH", "mylist{t}", "mylist{t}"), "c")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist{t}", "0", "-1")), "c", "a", "b")
}

func Test_TCL_tcl_lmove_right_left_base_case(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mylist1{t}", "mylist2{t}")
	cDo(t, client, "RPUSH", "mylist1{t}", "a", "b", "c", "d")
	assertBulkEq(t, cDo(t, client, "LMOVE", "mylist1{t}", "mylist2{t}", "RIGHT", "LEFT"), "d")
	assertBulkEq(t, cDo(t, client, "LMOVE", "mylist1{t}", "mylist2{t}", "RIGHT", "LEFT"), "c")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist1{t}", "0", "-1")), "a", "b")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist2{t}", "0", "-1")), "c", "d")
}

func Test_TCL_tcl_lmove_left_right_base_case(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mylist1{t}", "mylist2{t}")
	cDo(t, client, "RPUSH", "mylist1{t}", "a", "b", "c", "d")
	assertBulkEq(t, cDo(t, client, "LMOVE", "mylist1{t}", "mylist2{t}", "LEFT", "RIGHT"), "a")
	assertBulkEq(t, cDo(t, client, "LMOVE", "mylist1{t}", "mylist2{t}", "LEFT", "RIGHT"), "b")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist1{t}", "0", "-1")), "c", "d")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist2{t}", "0", "-1")), "a", "b")
}

func Test_TCL_tcl_lmpop_with_illegal_argument(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	assertErrorPrefix(t, cDo(t, client, "LMPOP"), "ERR")
	assertErrorPrefix(t, cDo(t, client, "LMPOP", "1"), "ERR")
	assertErrorPrefix(t, cDo(t, client, "LMPOP", "1", "mylist{t}"), "ERR")
	assertErrorPrefix(t, cDo(t, client, "LMPOP", "0", "mylist{t}", "LEFT"), "ERR")
	assertErrorPrefix(t, cDo(t, client, "LMPOP", "1", "mylist{t}", "bad_where"), "ERR")
	assertErrorPrefix(t, cDo(t, client, "LMPOP", "1", "mylist{t}", "LEFT", "COUNT", "0"), "ERR")
	assertErrorPrefix(t, cDo(t, client, "LMPOP", "1", "mylist{t}", "LEFT", "COUNT", "-1"), "ERR")
}

func Test_TCL_tcl_lrange_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mylist")
	for i := 0; i < 10; i++ {
		cDo(t, client, "RPUSH", "mylist", strconv.Itoa(i))
	}
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist", "1", "-2")), "1", "2", "3", "4", "5", "6", "7", "8")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist", "-3", "-1")), "7", "8", "9")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist", "4", "4")), "4")
}

func Test_TCL_tcl_lrange_out_of_range_indexes(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mylist")
	cDo(t, client, "RPUSH", "mylist", "a", "1", "2", "3")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist", "-1000", "1000")), "a", "1", "2", "3")
}

func Test_TCL_tcl_ltrim_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	trimList := func(min, max string) []string {
		cDo(t, client, "DEL", "mylist")
		cDo(t, client, "RPUSH", "mylist", "1", "2", "3", "4", "5")
		cDo(t, client, "LTRIM", "mylist", min, max)
		return extractBulkStrings(t, cDo(t, client, "LRANGE", "mylist", "0", "-1"))
	}
	assertStringsEq(t, trimList("0", "0"), "1")
	assertStringsEq(t, trimList("0", "1"), "1", "2")
	assertStringsEq(t, trimList("0", "2"), "1", "2", "3")
	assertStringsEq(t, trimList("1", "2"), "2", "3")
	assertStringsEq(t, trimList("1", "-1"), "2", "3", "4", "5")
	assertStringsEq(t, trimList("1", "-2"), "2", "3", "4")
	assertStringsEq(t, trimList("-2", "-1"), "4", "5")
	assertStringsEq(t, trimList("-1", "-1"), "5")
	assertStringsEq(t, trimList("-5", "-1"), "1", "2", "3", "4", "5")
	assertStringsEq(t, trimList("-10", "10"), "1", "2", "3", "4", "5")
}
