package tcl

// Mechanical port of frogdb crates/redis-regression/tests/other_regression.rs
// (Redis 8.6.0 unit/other_regression.tcl scenarios).

import (
	"testing"
)



func Test_TCL_leading_zeros_preserved_in_string_values(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "SET", "mykey", "007")
	_ = r
	assertOK(t, r)
	resp := cDo(t, client1, "GET", "mykey")
	_ = resp
	assertBulkEq(t, resp, "007")
	r = cDo(t, client1, "SET", "mykey", "00100")
	assertOK(t, r)
	resp = cDo(t, client1, "GET", "mykey")
	assertBulkEq(t, resp, "00100")
}
