package tcl

// Mechanical port of frogdb crates/redis-regression/tests/slowlog_tcl.rs
// (Redis 8.6.0 unit/slowlog.tcl scenarios).

import (
	"testing"
)

func Test_TCL_tcl_slowlog_check_that_it_starts_with_an_empty_log(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "SLOWLOG", "RESET")
	_ = r
	assertOK(t, r)
	r = cDo(t, client1, "SLOWLOG", "LEN")
	assertIntegerEq(t, r, 0)
}
























