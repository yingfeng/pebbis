package tcl

// Hand-ported tests from frogdb crates/redis-regression/tests/string_tcl.rs
// (Redis 8.6.0 unit/type/string.tcl). These were previously skip-only stubs
// emitted by the mechanical converter; they are now written by hand against
// the real Rust source.

import (
	"strconv"
	"strings"
	"testing"
)

func Test_TCL_tcl_very_big_payload_in_get_set(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	buf := strings.Repeat("abcd", 1_000_000)
	assertOK(t, cDo(t, client, "SET", "foo", buf))
	assertBulkEq(t, cDo(t, client, "GET", "foo"), buf)
}

func Test_TCL_tcl_mget_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "FLUSHDB")
	cDo(t, client, "SET", "foo{t}", "BAR")
	cDo(t, client, "SET", "bar{t}", "FOO")
	items := unwrapArray(t, cDo(t, client, "MGET", "foo{t}", "baazz{t}", "bar{t}"))
	assertBulkEq(t, items[0], "BAR")
	assertNil(t, items[1])
	assertBulkEq(t, items[2], "FOO")
}

func Test_TCL_tcl_mget_against_non_string_key(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "FLUSHDB")
	cDo(t, client, "SET", "foo{t}", "BAR")
	cDo(t, client, "SET", "bar{t}", "FOO")
	cDo(t, client, "SADD", "myset{t}", "ciao")
	cDo(t, client, "SADD", "myset{t}", "bau")
	items := unwrapArray(t, cDo(t, client, "MGET", "foo{t}", "baazz{t}", "bar{t}", "myset{t}"))
	assertBulkEq(t, items[0], "BAR")
	assertNil(t, items[1])
	assertBulkEq(t, items[2], "FOO")
	assertNil(t, items[3])
}

func Test_TCL_tcl_setrange_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mykey")
	assertIntegerEq(t, cDo(t, client, "SETRANGE", "mykey", "0", "foo"), 3)
	assertBulkEq(t, cDo(t, client, "GET", "mykey"), "foo")

	cDo(t, client, "DEL", "mykey")
	assertIntegerEq(t, cDo(t, client, "SETRANGE", "mykey", "0", ""), 0)
	assertIntegerEq(t, cDo(t, client, "EXISTS", "mykey"), 0)

	cDo(t, client, "DEL", "mykey")
	assertIntegerEq(t, cDo(t, client, "SETRANGE", "mykey", "1", "foo"), 4)
	assertBulkEq(t, cDo(t, client, "GET", "mykey"), "\x00foo")
}

func Test_TCL_tcl_setrange_with_out_of_range_offset(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "mykey")
	bigOffset := (512*1024*1024 - 4)
	assertErrorPrefix(t, cDo(t, client, "SETRANGE", "mykey", strconv.Itoa(bigOffset), "world"), "ERR")

	cDo(t, client, "SET", "mykey", "hello")
	assertErrorPrefix(t, cDo(t, client, "SETRANGE", "mykey", "-1", "world"), "ERR")
}
