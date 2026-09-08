package tcl

// Mechanical port of frogdb crates/redis-regression/tests/expire_regression.rs
// (Redis 8.6.0 unit scenarios).
import (
	"testing"
)
func Test_TCL_expire_conflicting_nx_xx_returns_error(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "mykey", "hello"))
	resp := cDoConn(t, client1, "EXPIRE", "mykey", "100", "NX", "XX")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_expire_conflicting_gt_lt_returns_error(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "mykey", "hello"))
	resp := cDoConn(t, client1, "EXPIRE", "mykey", "100", "GT", "LT")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_pexpire_large_ms_no_overflow(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "mykey", "hello"))
	resp := cDoConn(t, client1, "PEXPIRE", "mykey", "9223372036854775807")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_expiretime_returns_correct_seconds(t *testing.T) {
	t.Skip("TODO: only 9 of its statements could be ported")
}

func Test_TCL_pexpiretime_returns_exact_milliseconds(t *testing.T) {
	t.Skip("TODO: only 6 of its statements could be ported")
}
