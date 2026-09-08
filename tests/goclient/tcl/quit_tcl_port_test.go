package tcl

// Mechanical port of frogdb crates/redis-regression/tests/quit_tcl.rs
// (Redis 8.6.0 unit scenarios).
import (
	"testing"
)
func Test_TCL_tcl_quit_returns_ok(t *testing.T) {
	t.Skip("TODO: blocking client helper (blocker/writer) not supported by tcl harness")
}

func Test_TCL_tcl_pipelined_commands_after_quit_must_not_be_executed(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_pipelined_commands_after_quit_exceed_read_buffer(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}
