package tcl

// Mechanical port of frogdb crates/redis-regression/tests/info_tcl.rs
// (Redis 8.6.0 unit scenarios).
import (
	"testing"
)
func Test_TCL_errorstats_wrongtype_is_failed_call(t *testing.T) {
	t.Skip("TODO: only 9 of its statements could be ported")
}

func Test_TCL_errorstats_wrong_arity_is_rejected_call(t *testing.T) {
	t.Skip("TODO: converter: frogdb info_section() helper has no Go counterpart")
}

func Test_TCL_errorstats_total_error_replies_sums_across_types(t *testing.T) {
	t.Skip("TODO: only 9 of its statements could be ported")
}

func Test_TCL_errorstats_nogroup_is_failed_call(t *testing.T) {
	t.Skip("TODO: only 10 of its statements could be ported")
}

func Test_TCL_errorstats_nopermission_is_rejected_call(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_errorstats_unknown_command_is_rejected_call(t *testing.T) {
	t.Skip("TODO: only 8 of its statements could be ported")
}

func Test_TCL_commandstats_unknown_command_names_do_not_grow_cmdstat_entries(t *testing.T) {
	t.Skip("TODO: only 15 of its statements could be ported")
}

func Test_TCL_errorstats_oom_is_rejected_call(t *testing.T) {
	t.Skip("TODO: converter: cannot express Rust to_string()")
}

func Test_TCL_errorstats_auth_failure_is_failed_call(t *testing.T) {
	t.Skip("TODO: only 9 of its statements could be ported")
}

func Test_TCL_errorstats_multi_exec_errors_are_recorded(t *testing.T) {
	t.Skip("TODO: only 13 of its statements could be ported")
}

func Test_TCL_errorstats_evalsha_noscript_is_failed_call(t *testing.T) {
	t.Skip("TODO: only 12 of its statements could be ported")
}
