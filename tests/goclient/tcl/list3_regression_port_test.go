package tcl

// Mechanical port of frogdb crates/redis-regression/tests/list3_regression.rs
// (Redis 8.6.0 unit scenarios).
import (
	"testing"
)
func Test_TCL_lrange_negative_indices_backward_traversal(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "RPUSH", "mylist", "a", "b", "c", "d", "e")
	resp := cDoConn(t, client1, "LRANGE", "mylist", "-2", "-1")
	items := unwrapArray(t, resp)
	if len(items) != 2 { t.Fatalf("len mismatch: got %d want 2", len(items)) }
	assertBulkEq(t, items[0], "d")
	assertBulkEq(t, items[1], "e")
	resp = cDoConn(t, client1, "LRANGE", "mylist", "-1", "-1")
	items = unwrapArray(t, resp)
	if len(items) != 1 { t.Fatalf("len mismatch: got %d want 1", len(items)) }
	assertBulkEq(t, items[0], "e")
}

func Test_TCL_lpush_rpush_interleaving_preserves_order(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "RPUSH", "mylist", "mid")
	cDoConn(t, client1, "LPUSH", "mylist", "left1")
	cDoConn(t, client1, "RPUSH", "mylist", "right1")
	cDoConn(t, client1, "LPUSH", "mylist", "left2")
	cDoConn(t, client1, "RPUSH", "mylist", "right2")
	resp := cDoConn(t, client1, "LRANGE", "mylist", "0", "-1")
	items := unwrapArray(t, resp)
	if len(items) != 5 { t.Fatalf("len mismatch: got %d want 5", len(items)) }
	assertBulkEq(t, items[0], "left2")
	assertBulkEq(t, items[1], "left1")
	assertBulkEq(t, items[2], "mid")
	assertBulkEq(t, items[3], "right1")
	assertBulkEq(t, items[4], "right2")
}

func Test_TCL_integer_values_various_sizes(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	values := []string{"0", "127", "-128", "32767", "-32768", "2147483647", "-2147483648", "9223372036854775807", "-9223372036854775808"}
	for _, v := range values {
		_ = v
	cDoConn(t, client1, "RPUSH", "mylist", v)
	}
	resp := cDoConn(t, client1, "LRANGE", "mylist", "0", "-1")
	items := unwrapArray(t, resp)
	_ = items
	// PORT-TODO: assert_eq!(items.len(), values.len());
	for i, v := range values {
		_ = i
		_ = v
	// PORT-TODO: assert_bulk_eq(&items[i], v.as_bytes());
	}
}

func Test_TCL_edge_case_integer_values_roundtrip(t *testing.T) {
	t.Skip("TODO: only 7 of its statements could be ported")
}
