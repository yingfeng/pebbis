package tcl

// Mechanical port of frogdb crates/redis-regression/tests/other_tcl.rs
// (Redis 8.6.0 unit scenarios).
import (
	"testing"
)
func Test_TCL_tcl_coverage_help_commands(t *testing.T) {
	t.Skip("TODO: only 7 of its statements could be ported")
}

func Test_TCL_tcl_coverage_memory_purge(t *testing.T) {
	t.Skip("TODO: redistore: MEMORY maintenance subcommand (management surface, out of scope)")
}

func Test_TCL_tcl_pipelining_stresser(t *testing.T) {
	t.Skip("TODO: only 13 of its statements could be ported")
}

func Test_TCL_tcl_append_basics(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "foo")
	cDoConn(t, client1, "APPEND", "foo", "bar")
	cDoConn(t, client1, "GET", "foo")
	cDoConn(t, client1, "APPEND", "foo", "100")
	cDoConn(t, client1, "GET", "foo")
}

func Test_TCL_tcl_append_basics_integer_encoded_values(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "foo")
	cDoConn(t, client1, "APPEND", "foo", "1")
	cDoConn(t, client1, "APPEND", "foo", "2")
	cDoConn(t, client1, "GET", "foo")
	cDoConn(t, client1, "SET", "foo", "1")
	cDoConn(t, client1, "APPEND", "foo", "2")
	cDoConn(t, client1, "GET", "foo")
}

func Test_TCL_tcl_append_fuzzing(t *testing.T) {
	t.Skip("TODO: only 13 of its statements could be ported")
}

func Test_TCL_tcl_flushdb(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "a", "1")
	cDoConn(t, client1, "SET", "b", "2")
	cDoConn(t, client1, "SET", "c", "3")
	assertOK(t, cDoConn(t, client1, "FLUSHDB"))
	cDoConn(t, client1, "DBSIZE")
}

func Test_TCL_tcl_subcommand_syntax_error_crash_issue_10070(t *testing.T) {
	t.Skip("TODO: only 8 of its statements could be ported")
}
