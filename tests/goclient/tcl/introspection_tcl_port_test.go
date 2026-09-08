package tcl

// Mechanical port of frogdb crates/redis-regression/tests/introspection_tcl.rs
// (Redis 8.6.0 unit scenarios).
import (
	"testing"
)
func Test_TCL_tcl_ping(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	r := cDoConn(t, client1, "PING")
	// PORT-TODO: assert!(
	// PORT-TODO: matches!(&r, Response::Simple(s) if s == "PONG"),
	// PORT-TODO: "expected PONG, got {r:?}"
	// PORT-TODO: );
	cDoConn(t, client1, "PING", "redis")
	r = cDoConn(t, client1, "PING", "hello", "redis")
	assertErrorPrefix(t, r, "ERR wrong number of arguments for 'ping' command")
}

func Test_TCL_tcl_client_list(t *testing.T) {
	t.Skip("TODO: only 4 of its statements could be ported")
}

func Test_TCL_tcl_client_list_with_ids(t *testing.T) {
	t.Skip("TODO: only 7 of its statements could be ported")
}

func Test_TCL_tcl_client_info(t *testing.T) {
	t.Skip("TODO: only 4 of its statements could be ported")
}

func Test_TCL_tcl_client_info_has_the_redis_8_6_field_set(t *testing.T) {
	t.Skip("TODO: only 6 of its statements could be ported")
}

func Test_TCL_tcl_client_list_reports_real_watch_count(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_client_kill_illegal_arguments(t *testing.T) {
	t.Skip("TODO: only 8 of its statements could be ported")
}

func Test_TCL_tcl_client_kill_skipme_yes_kills_other_clients(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_client_getname_returns_nil_if_not_assigned(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "CLIENT", "GETNAME")
}

func Test_TCL_tcl_client_getname_returns_name_after_setname(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "CLIENT", "SETNAME", "testName"))
	cDoConn(t, client1, "CLIENT", "GETNAME")
}

func Test_TCL_tcl_client_list_shows_empty_name_for_unassigned(t *testing.T) {
	t.Skip("TODO: only 4 of its statements could be ported")
}

func Test_TCL_tcl_client_setname_does_not_accept_spaces(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	r := cDoConn(t, client1, "CLIENT", "SETNAME", "foo bar")
	_ = r
	// PORT-TODO: assert!(
	// PORT-TODO: matches!(&r, Response::Error(_)),
	// PORT-TODO: "expected error for name with space"
	// PORT-TODO: );
}

func Test_TCL_tcl_client_setname_assigns_name(t *testing.T) {
	t.Skip("TODO: only 5 of its statements could be ported")
}

func Test_TCL_tcl_client_setname_can_change_name(t *testing.T) {
	t.Skip("TODO: only 6 of its statements could be ported")
}

func Test_TCL_tcl_client_setname_connection_can_be_closed(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_client_setinfo_lib_name_and_ver(t *testing.T) {
	t.Skip("TODO: only 6 of its statements could be ported")
}

func Test_TCL_tcl_client_setinfo_invalid_args(t *testing.T) {
	t.Skip("TODO: only 5 of its statements could be ported")
}

func Test_TCL_tcl_client_setinfo_can_clear_lib_name(t *testing.T) {
	t.Skip("TODO: only 6 of its statements could be ported")
}

func Test_TCL_tcl_client_id_returns_integer(t *testing.T) {
	t.Skip("TODO: only 4 of its statements could be ported")
}

func Test_TCL_tcl_client_ids_are_unique(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_client_no_evict_syntax_error(t *testing.T) {
	t.Skip("TODO: Pebbis: CLIENT introspection (management surface, out of scope)")
}

func Test_TCL_tcl_client_no_evict_on_off(t *testing.T) {
	t.Skip("TODO: Pebbis: CLIENT introspection (management surface, out of scope)")
}

func Test_TCL_tcl_client_kill_by_id(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_client_kill_no_such_addr(t *testing.T) {
	t.Skip("TODO: only 4 of its statements could be ported")
}

func Test_TCL_tcl_client_pause_invalid_timeout(t *testing.T) {
	t.Skip("TODO: only 4 of its statements could be ported")
}

func Test_TCL_tcl_command_count(t *testing.T) {
	t.Skip("TODO: only 4 of its statements could be ported")
}

func Test_TCL_tcl_command_list(t *testing.T) {
	t.Skip("TODO: only 5 of its statements could be ported")
}

func Test_TCL_tcl_command_list_count_matches(t *testing.T) {
	t.Skip("TODO: only 5 of its statements could be ported")
}

func Test_TCL_tcl_command_docs_get(t *testing.T) {
	t.Skip("TODO: only 10 of its statements could be ported")
}

func Test_TCL_tcl_command_docs_extension_omits_unknown_complexity(t *testing.T) {
	t.Skip("TODO: only 6 of its statements could be ported")
}

func Test_TCL_tcl_command_docs_unknown_is_skipped(t *testing.T) {
	t.Skip("TODO: Pebbis: COMMAND introspection (management surface, out of scope)")
}

func Test_TCL_tcl_command_docs_all_commands(t *testing.T) {
	t.Skip("TODO: converter: cannot express Rust iterator adapter")
}

func Test_TCL_tcl_config_get_returns_pairs(t *testing.T) {
	t.Skip("TODO: only 6 of its statements could be ported")
}

func Test_TCL_tcl_config_get_wildcard(t *testing.T) {
	t.Skip("TODO: only 4 of its statements could be ported")
}

func Test_TCL_tcl_config_get_multiple_patterns(t *testing.T) {
	t.Skip("TODO: only 4 of its statements could be ported")
}

func Test_TCL_tcl_config_set_duplicate_error(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	r := cDoConn(t, client1, "CONFIG", "SET", "maxmemory", "10000001", "maxmemory", "10000002")
	_ = r
	// PORT-TODO: assert!(
	// PORT-TODO: matches!(&r, Response::Error(_)),
	// PORT-TODO: "CONFIG SET with duplicate keys should error"
	// PORT-TODO: );
}

func Test_TCL_tcl_object_help(t *testing.T) {
	t.Skip("TODO: Pebbis: command HELP (management surface, out of scope)")
}

func Test_TCL_tcl_object_encoding_string(t *testing.T) {
	t.Skip("TODO: only 5 of its statements could be ported")
}

func Test_TCL_tcl_object_encoding_int(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "mykey", "12345")
	r := cDoConn(t, client1, "OBJECT", "ENCODING", "mykey")
	_ = r
	var enc reply
	_ = enc
	// PORT-TODO let enc = String::from_utf8(unwrap_bulk(&r).to_vec()).unwrap()
	// PORT-TODO: assert_eq!(enc, "int", "integer string encoding should be int");
}

func Test_TCL_tcl_object_encoding_list(t *testing.T) {
	t.Skip("TODO: only 5 of its statements could be ported")
}

func Test_TCL_tcl_object_encoding_set(t *testing.T) {
	t.Skip("TODO: only 5 of its statements could be ported")
}

func Test_TCL_tcl_object_encoding_hash(t *testing.T) {
	t.Skip("TODO: only 5 of its statements could be ported")
}

func Test_TCL_tcl_object_encoding_zset(t *testing.T) {
	t.Skip("TODO: only 5 of its statements could be ported")
}

func Test_TCL_tcl_object_encoding_nonexistent_key(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	r := cDoConn(t, client1, "OBJECT", "ENCODING", "nosuchkey")
	_ = r
	// PORT-TODO: assert!(
	// PORT-TODO: matches!(&r, Response::Bulk(None)),
	// PORT-TODO: "OBJECT ENCODING on nonexistent key should return nil, got {r:?}"
	// PORT-TODO: );
}

func Test_TCL_tcl_dbsize_empty(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DBSIZE")
}

func Test_TCL_tcl_dbsize_after_inserts(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "k1", "v1")
	cDoConn(t, client1, "SET", "k2", "v2")
	cDoConn(t, client1, "SET", "k3", "v3")
	cDoConn(t, client1, "DBSIZE")
}

func Test_TCL_tcl_dbsize_after_delete(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "k1", "v1")
	cDoConn(t, client1, "SET", "k2", "v2")
	cDoConn(t, client1, "DEL", "k1")
	cDoConn(t, client1, "DBSIZE")
}

func Test_TCL_tcl_client_reply_skip(t *testing.T) {
	t.Skip("TODO: only 3 of its statements could be ported")
}

func Test_TCL_tcl_client_reply_on_unsets_skip(t *testing.T) {
	t.Skip("TODO: only 3 of its statements could be ported")
}

func Test_TCL_tcl_client_reply_bad_argument(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	r := cDoConn(t, client1, "CLIENT", "REPLY", "wrongInput")
	_ = r
	// PORT-TODO: assert!(
	// PORT-TODO: matches!(&r, Response::Error(_)),
	// PORT-TODO: "expected error for CLIENT REPLY wrongInput, got {r:?}"
	// PORT-TODO: );
}

func Test_TCL_tcl_client_info_stats_for_blocking_command(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_config_sanity(t *testing.T) {
	t.Skip("TODO: converter: cannot express Rust iterator adapter")
}

func Test_TCL_tcl_config_during_loading(t *testing.T) {
	t.Skip("TODO: only 15 of its statements could be ported")
}

func Test_TCL_tcl_client_reply_off_on(t *testing.T) {
	t.Skip("TODO: only 3 of its statements could be ported")
}

func Test_TCL_tcl_client_command_unhappy_path_coverage(t *testing.T) {
	t.Skip("TODO: only 21 of its statements could be ported")
}

func Test_TCL_tcl_reset_does_not_clean_library_name(t *testing.T) {
	t.Skip("TODO: only 10 of its statements could be ported")
}

func Test_TCL_tcl_argument_rewriting_issue_9598(t *testing.T) {
	t.Skip("TODO: only 19 of its statements could be ported")
}
