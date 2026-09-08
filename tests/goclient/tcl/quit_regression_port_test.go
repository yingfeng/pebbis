package tcl

// Mechanical port of frogdb crates/redis-regression/tests/quit_regression.rs
// (Redis 8.6.0 unit/quit_regression.tcl scenarios).

import (
	"testing"
)

func Test_TCL_quit_returns_ok(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "QUIT")
	_ = resp
	assertOK(t, resp)
}






