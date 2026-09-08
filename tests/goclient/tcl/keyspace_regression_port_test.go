package tcl

// Mechanical port of frogdb crates/redis-regression/tests/keyspace_regression.rs
// (Redis 8.6.0 unit scenarios).
import (
	"testing"
)
func Test_TCL_copy_with_invalid_destination_db_returns_error(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "{k}src", "hello"))
	resp := cDoConn(t, client1, "COPY", "{k}src", "{k}dst", "DB", "notanumber")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_keys_with_backtracking_glob_matches_correctly(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "hellofoobarbaz", "1"))
	assertOK(t, cDoConn(t, client1, "SET", "foobarbaz", "1"))
	assertOK(t, cDoConn(t, client1, "SET", "foobar", "1"))
	assertOK(t, cDoConn(t, client1, "SET", "nope", "1"))
	resp := cDoConn(t, client1, "KEYS", "*foo*bar*")
	keys := extractBulkStrings(t, resp)
	// PORT-TODO: keys.sort();
	assertStringsEq(t, keys, "foobar", "foobarbaz", "hellofoobarbaz")
}

func Test_TCL_copy_basic_same_slot(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "{k}src", "hello"))
	resp := cDoConn(t, client1, "COPY", "{k}src", "{k}dst")
	_ = resp
	// PORT-TODO: assert_eq!(unwrap_integer(&resp), 1);
	cDoConn(t, client1, "GET", "{k}dst")
}

func Test_TCL_copy_returns_zero_when_dest_exists_without_replace(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "{k}src", "hello"))
	assertOK(t, cDoConn(t, client1, "SET", "{k}dst", "existing"))
	resp := cDoConn(t, client1, "COPY", "{k}src", "{k}dst")
	_ = resp
	// PORT-TODO: assert_eq!(unwrap_integer(&resp), 0);
	cDoConn(t, client1, "GET", "{k}dst")
}

func Test_TCL_copy_replace_overwrites_destination(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "{k}src", "new_value"))
	assertOK(t, cDoConn(t, client1, "SET", "{k}dst", "old_value"))
	resp := cDoConn(t, client1, "COPY", "{k}src", "{k}dst", "REPLACE")
	_ = resp
	// PORT-TODO: assert_eq!(unwrap_integer(&resp), 1);
	cDoConn(t, client1, "GET", "{k}dst")
}

func Test_TCL_copy_nonexistent_source_returns_zero(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	resp := cDoConn(t, client1, "COPY", "{k}nosrc", "{k}dst")
	_ = resp
	// PORT-TODO: assert_eq!(unwrap_integer(&resp), 0);
}

func Test_TCL_copy_preserves_ttl_on_source(t *testing.T) {
	t.Skip("TODO: only 7 of its statements could be ported")
}

func Test_TCL_copy_hash_type(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "HSET", "{k}src", "f1", "v1", "f2", "v2")
	resp := cDoConn(t, client1, "COPY", "{k}src", "{k}dst")
	_ = resp
	// PORT-TODO: assert_eq!(unwrap_integer(&resp), 1);
	cDoConn(t, client1, "HGET", "{k}dst", "f1")
	cDoConn(t, client1, "HGET", "{k}dst", "f2")
}

func Test_TCL_copy_list_type(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "RPUSH", "{k}src", "a", "b", "c")
	resp := cDoConn(t, client1, "COPY", "{k}src", "{k}dst")
	_ = resp
	// PORT-TODO: assert_eq!(unwrap_integer(&resp), 1);
	// PORT-TODO: assert_eq!(
	cDoConn(t, client1, "LLEN", "{k}dst")
	// PORT-TODO: 3
	// PORT-TODO: );
	cDoConn(t, client1, "LINDEX", "{k}dst", "0")
}

func Test_TCL_copy_json_type(t *testing.T) {
	t.Skip("TODO: converter: cannot express Rust raw string literal")
}
