package tcl

// Mechanical port of frogdb crates/redis-regression/tests/stream_regression.rs
// (Redis 8.6.0 unit/stream_regression.tcl scenarios).

import (
	"testing"
)





func Test_TCL_xadd_rejects_non_monotonic_explicit_ids(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "XADD", "stream", "5-0", "k", "v")
	resp := cDo(t, client1, "XADD", "stream", "3-0", "k", "v")
	_ = resp
	assertErrorPrefix(t, resp, "ERR")
}












