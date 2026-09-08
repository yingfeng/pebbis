package tcl

// Mechanical port of frogdb crates/redis-regression/tests/auth_tcl.rs
// (Redis 8.6.0 unit/auth.tcl scenarios).

import (
	"testing"
)

func Test_TCL_tcl_auth_fails_if_no_password_configured(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "AUTH", "foo")
	_ = resp
	assertErrorPrefix(t, resp, "ERR")
}



















