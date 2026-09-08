package tcl

// Mechanical port of frogdb crates/redis-regression/tests/protocol_tcl.rs
// (Redis 8.6.0 unit/protocol.tcl scenarios).

import (
	"testing"
)

















func Test_TCL_tcl_generic_wrong_number_of_args(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "PING", "x", "y", "z")
	_ = resp
	assertErrorPrefix(t, resp, "ERR")
}









































