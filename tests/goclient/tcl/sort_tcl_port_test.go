package tcl

// Mechanical port of frogdb crates/redis-regression/tests/sort_tcl.rs
// (Redis 8.6.0 unit scenarios).
import (
	"fmt"
	"testing"
)
func Test_TCL_tcl_sort_get_hash_returns_elements_sorted(t *testing.T) {
	t.Skip("TODO: converter: cannot express Rust to_string()")
}

func Test_TCL_tcl_sort_get_const_returns_nils(t *testing.T) {
	t.Skip("TODO: converter: cannot express Rust to_string()")
}

func Test_TCL_tcl_sort_ro_get_const_returns_nils(t *testing.T) {
	t.Skip("TODO: converter: cannot express Rust to_string()")
}

func Test_TCL_tcl_sort_desc(t *testing.T) {
	t.Skip("TODO: converter: cannot express Rust to_string()")
}

func Test_TCL_tcl_sort_alpha_against_integer_encoded_strings(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mylist")
	cDoConn(t, client1, "LPUSH", "mylist", "2")
	cDoConn(t, client1, "LPUSH", "mylist", "1")
	cDoConn(t, client1, "LPUSH", "mylist", "3")
	cDoConn(t, client1, "LPUSH", "mylist", "10")
	resp := cDoConn(t, client1, "SORT", "mylist", "ALPHA")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "1", "10", "2", "3")
}

func Test_TCL_tcl_sort_sorted_set(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "zset")
	cDoConn(t, client1, "ZADD", "zset", "1", "a")
	cDoConn(t, client1, "ZADD", "zset", "5", "b")
	cDoConn(t, client1, "ZADD", "zset", "2", "c")
	cDoConn(t, client1, "ZADD", "zset", "10", "d")
	cDoConn(t, client1, "ZADD", "zset", "3", "e")
	resp := cDoConn(t, client1, "SORT", "zset", "ALPHA", "DESC")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "e", "d", "c", "b", "a")
}

func Test_TCL_tcl_sort_sorted_set_by_nosort_retains_ordering(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "zset")
	cDoConn(t, client1, "ZADD", "zset", "1", "a")
	cDoConn(t, client1, "ZADD", "zset", "5", "b")
	cDoConn(t, client1, "ZADD", "zset", "2", "c")
	cDoConn(t, client1, "ZADD", "zset", "10", "d")
	cDoConn(t, client1, "ZADD", "zset", "3", "e")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	cDoConn(t, client1, "SORT", "zset", "BY", "nosort", "ASC")
	cDoConn(t, client1, "SORT", "zset", "BY", "nosort", "DESC")
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	asc := extractBulkStrings(t, results[0])
	desc := extractBulkStrings(t, results[1])
	assertStringsEq(t, asc, "a", "c", "e", "b", "d")
	assertStringsEq(t, desc, "d", "b", "e", "c", "a")
}

func Test_TCL_tcl_sort_sorted_set_by_nosort_with_limit(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "zset")
	cDoConn(t, client1, "ZADD", "zset", "1", "a")
	cDoConn(t, client1, "ZADD", "zset", "5", "b")
	cDoConn(t, client1, "ZADD", "zset", "2", "c")
	cDoConn(t, client1, "ZADD", "zset", "10", "d")
	cDoConn(t, client1, "ZADD", "zset", "3", "e")
	resp := cDoConn(t, client1, "SORT", "zset", "BY", "nosort", "ASC", "LIMIT", "0", "1")
	// PORT-TODO: assert_eq!(extract_bulk_strings(&resp), vec!["a"]);
	resp = cDoConn(t, client1, "SORT", "zset", "BY", "nosort", "DESC", "LIMIT", "0", "1")
	// PORT-TODO: assert_eq!(extract_bulk_strings(&resp), vec!["d"]);
	resp = cDoConn(t, client1, "SORT", "zset", "BY", "nosort", "ASC", "LIMIT", "0", "2")
	// PORT-TODO: assert_eq!(extract_bulk_strings(&resp), vec!["a", "c"]);
	resp = cDoConn(t, client1, "SORT", "zset", "BY", "nosort", "DESC", "LIMIT", "0", "2")
	// PORT-TODO: assert_eq!(extract_bulk_strings(&resp), vec!["d", "b"]);
	resp = cDoConn(t, client1, "SORT", "zset", "BY", "nosort", "LIMIT", "5", "10")
	items := unwrapArray(t, resp)
	_ = items
	// PORT-TODO: assert!(
	// PORT-TODO: items.is_empty(),
	// PORT-TODO: "expected empty array for out-of-range LIMIT"
	// PORT-TODO: );
	resp = cDoConn(t, client1, "SORT", "zset", "BY", "nosort", "LIMIT", "-10", "100")
	// PORT-TODO: assert_eq!(extract_bulk_strings(&resp), vec!["a", "c", "e", "b", "d"]);
}

func Test_TCL_tcl_sort_sorted_set_inf_handling(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "zset")
	cDoConn(t, client1, "ZADD", "zset", "-100", "a")
	cDoConn(t, client1, "ZADD", "zset", "200", "b")
	cDoConn(t, client1, "ZADD", "zset", "-300", "c")
	cDoConn(t, client1, "ZADD", "zset", "1000000", "d")
	cDoConn(t, client1, "ZADD", "zset", "+inf", "max")
	cDoConn(t, client1, "ZADD", "zset", "-inf", "min")
	resp := cDoConn(t, client1, "ZRANGE", "zset", "0", "-1")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "min", "c", "a", "b", "d", "max")
}

func Test_TCL_tcl_sort_regression_issue_19_sorting_floats(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "FLUSHDB")
	floats := []string{"1.1", "5.10", "3.10", "7.44", "2.1", "5.75", "6.12", "0.25", "1.15"}
	for _, x := range floats {
		_ = x
	cDoConn(t, client1, "LPUSH", "mylist", x)
	}
	resp := cDoConn(t, client1, "SORT", "mylist")
	items := extractBulkStrings(t, resp)
	_ = items
	// PORT-TODO: let mut sorted_floats: Vec<f64> = floats.iter().map(|s| s.parse().unwrap()).collect();
	// PORT-TODO: sorted_floats.sort_by(|a, b| a.partial_cmp(b).unwrap());
	var _expected []string
	_ = _expected
	// PORT-TODO: let _expected: Vec<String> = sorted_floats.iter().map(|f| format!("{f}")).collect();
	var actual_f []string
	_ = actual_f
	// PORT-TODO: let actual_f: Vec<f64> = items.iter().map(|s| s.parse().unwrap()).collect();
	// PORT-TODO: assert_eq!(actual_f.len(), sorted_floats.len());
	// PORT-TODO: for (a, e) in actual_f.iter().zip(sorted_floats.iter()) {
	// PORT-TODO: assert!(
	// PORT-TODO: (a - e).abs() < 1e-10,
	// PORT-TODO: "float mismatch: got {a}, expected {e}"
	// PORT-TODO: );
	// PORT-TODO: }
}

func Test_TCL_tcl_sort_store_returns_zero_if_empty(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "FLUSHDB")
	// PORT-TODO: assert_integer_eq(
	cDoConn(t, client1, "SORT", "{t}foo", "STORE", "{t}bar")
	// PORT-TODO: 0,
	// PORT-TODO: );
}

func Test_TCL_tcl_sort_store_does_not_create_empty_lists(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "FLUSHDB")
	cDoConn(t, client1, "LPUSH", "foo", "bar")
	cDoConn(t, client1, "SORT", "foo", "ALPHA", "LIMIT", "10", "10", "STORE", "zap")
	cDoConn(t, client1, "EXISTS", "zap")
}

func Test_TCL_tcl_sort_store_removes_key_if_result_empty(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "FLUSHDB")
	cDoConn(t, client1, "LPUSH", "{t}foo", "bar")
	cDoConn(t, client1, "SORT", "{t}emptylist", "STORE", "{t}foo")
	cDoConn(t, client1, "EXISTS", "{t}foo")
}

func Test_TCL_tcl_sort_by_constant_store_orders_output(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "{t}myset", "{t}mylist")
	members := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "z", "aa", "aaa", "azz"}
	for _, m := range members {
		_ = m
	cDoConn(t, client1, "SADD", "{t}myset", m)
	}
	cDoConn(t, client1, "SORT", "{t}myset", "ALPHA", "BY", "_", "STORE", "{t}mylist")
	resp := cDoConn(t, client1, "LRANGE", "{t}mylist", "0", "-1")
	items := extractBulkStrings(t, resp)
	expected := []string{"a", "aa", "aaa", "azz", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "z"}
	assertStringsEqVar(t, items, expected)
}

func Test_TCL_tcl_sort_complains_bad_double_in_set(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "myset")
	cDoConn(t, client1, "SADD", "myset", "1", "2", "3", "4", "not-a-double")
	resp := cDoConn(t, client1, "SORT", "myset")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_sort_complains_bad_double_in_by_key(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "{t}myset")
	cDoConn(t, client1, "SADD", "{t}myset", "1", "2", "3", "4")
	cDoConn(t, client1, "MSET", "{t}score:1", "10", "{t}score:2", "20", "{t}score:3", "30", "{t}score:4", "not-a-double")
	resp := cDoConn(t, client1, "SORT", "{t}myset", "BY", "{t}score:*")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_sort_get_pattern_ending_with_arrow(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "{t}mylist")
	cDoConn(t, client1, "LPUSH", "{t}mylist", "a")
	cDoConn(t, client1, "SET", "{t}x:a->", "100")
	resp := cDoConn(t, client1, "SORT", "{t}mylist", "BY", "num", "GET", "{t}x:*->")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "100")
}

func Test_TCL_tcl_sort_by_nosort_retains_list_order(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "testa")
	cDoConn(t, client1, "LPUSH", "testa", "2", "1", "4", "3", "5")
	resp := cDoConn(t, client1, "SORT", "testa", "BY", "nosort")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "5", "3", "4", "1", "2")
}

func Test_TCL_tcl_sort_by_nosort_store_retains_list_order(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "{t}testa", "{t}testb")
	cDoConn(t, client1, "LPUSH", "{t}testa", "2", "1", "4", "3", "5")
	cDoConn(t, client1, "SORT", "{t}testa", "BY", "nosort", "STORE", "{t}testb")
	resp := cDoConn(t, client1, "LRANGE", "{t}testb", "0", "-1")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "5", "3", "4", "1", "2")
}

func Test_TCL_tcl_sort_by_nosort_with_limit(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "{t}testa", "{t}testb")
	cDoConn(t, client1, "LPUSH", "{t}testa", "2", "1", "4", "3", "5")
	cDoConn(t, client1, "SORT", "{t}testa", "BY", "nosort", "LIMIT", "0", "3", "STORE", "{t}testb")
	resp := cDoConn(t, client1, "LRANGE", "{t}testb", "0", "-1")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "5", "3", "4")
}

func Test_TCL_tcl_sort_ro_successful(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "{t}mylist")
	cDoConn(t, client1, "LPUSH", "{t}mylist", "a")
	cDoConn(t, client1, "SET", "{t}x:a->", "100")
	resp := cDoConn(t, client1, "SORT_RO", "{t}mylist", "BY", "nosort", "GET", "{t}x:*->")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "100")
}

func Test_TCL_tcl_sort_ro_cannot_use_store(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	resp := cDoConn(t, client1, "SORT_RO", "foolist", "STORE", "bar")
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_sort_ro_huge_limit_offset(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "L")
	cDoConn(t, client1, "LPUSH", "L", "2", "1", "0")
	resp := cDoConn(t, client1, "SORT_RO", "L", "BY", "a", "LIMIT", "2", "9223372036854775807")
	items := unwrapArray(t, resp)
	_ = items
	// PORT-TODO assert: items.len() <= 1
	// PORT-TODO: }
	// PORT-TODO: Response::Error(_) => {
	// PORT-TODO: }
	// PORT-TODO: other => panic!("expected Array or Error, got {other:?}"),
	// PORT-TODO: }
}

func Test_TCL_tcl_sort_by_external_key(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "{t}tosort")
	data := [][2]string{{"0", "30"}, {"1", "10"}, {"2", "20"}}
	for _, __pr := range data {
		elem, weight := __pr[0], __pr[1]
	cDoConn(t, client1, "LPUSH", "{t}tosort", elem)
	cDoConn(t, client1, "SET", fmt.Sprintf("{t}weight_%s", elem), weight)
	}
	resp := cDoConn(t, client1, "SORT", "{t}tosort", "BY", "{t}weight_*")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "1", "2", "0")
}

func Test_TCL_tcl_sort_by_external_key_with_limit(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "tosort")
	data := [][2]string{{"0", "30"}, {"1", "10"}, {"2", "20"}, {"3", "5"}, {"4", "25"}}
	for _, __pr := range data {
		elem, weight := __pr[0], __pr[1]
	cDoConn(t, client1, "LPUSH", "tosort", elem)
	cDoConn(t, client1, "SET", fmt.Sprintf("weight_%s", elem), weight)
	}
	resp := cDoConn(t, client1, "SORT", "tosort", "BY", "weight_*", "LIMIT", "1", "2")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "1", "2")
}

func Test_TCL_tcl_sort_by_hash_field(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "{t}tosort")
	data := [][2]string{{"0", "30"}, {"1", "10"}, {"2", "20"}}
	for _, __pr := range data {
		elem, weight := __pr[0], __pr[1]
	cDoConn(t, client1, "LPUSH", "{t}tosort", elem)
	cDoConn(t, client1, "HSET", fmt.Sprintf("{t}wobj_%s", elem), "weight", weight)
	}
	resp := cDoConn(t, client1, "SORT", "{t}tosort", "BY", "{t}wobj_*->weight")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "1", "2", "0")
}

func Test_TCL_tcl_sort_get_key_and_hash_sanity_check(t *testing.T) {
	t.Skip("TODO: converter: cannot express Rust step_by() iteration")
}

func Test_TCL_tcl_sort_by_key_store(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "{t}tosort", "{t}sort-res")
	data := [][2]string{{"0", "30"}, {"1", "10"}, {"2", "20"}}
	for _, __pr := range data {
		elem, weight := __pr[0], __pr[1]
	cDoConn(t, client1, "LPUSH", "{t}tosort", elem)
	cDoConn(t, client1, "SET", fmt.Sprintf("{t}weight_%s", elem), weight)
	}
	cDoConn(t, client1, "SORT", "{t}tosort", "BY", "{t}weight_*", "STORE", "{t}sort-res")
	resp := cDoConn(t, client1, "LRANGE", "{t}sort-res", "0", "-1")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "1", "2", "0")
	cDoConn(t, client1, "LLEN", "{t}sort-res")
}

func Test_TCL_tcl_sort_by_hash_field_store(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "{t}tosort", "{t}sort-res")
	data := [][2]string{{"0", "30"}, {"1", "10"}, {"2", "20"}}
	for _, __pr := range data {
		elem, weight := __pr[0], __pr[1]
	cDoConn(t, client1, "LPUSH", "{t}tosort", elem)
	cDoConn(t, client1, "HSET", fmt.Sprintf("{t}wobj_%s", elem), "weight", weight)
	}
	cDoConn(t, client1, "SORT", "{t}tosort", "BY", "{t}wobj_*->weight", "STORE", "{t}sort-res")
	resp := cDoConn(t, client1, "LRANGE", "{t}sort-res", "0", "-1")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "1", "2", "0")
	cDoConn(t, client1, "LLEN", "{t}sort-res")
}

func Test_TCL_tcl_sort_extracts_store_correctly(t *testing.T) {
	t.Skip("TODO: redistore: COMMAND GETKEYS not implemented (management surface, out of scope)")
}

func Test_TCL_tcl_sort_ro_get_keys(t *testing.T) {
	t.Skip("TODO: redistore: COMMAND GETKEYS not implemented (management surface, out of scope)")
}

func Test_TCL_tcl_sort_extracts_multiple_store_correctly(t *testing.T) {
	t.Skip("TODO: redistore: COMMAND GETKEYS not implemented (management surface, out of scope)")
}

func Test_TCL_tcl_sort_by_subsorts_lexicographically_on_tie(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "{t}myset")
	members := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "z", "aa", "aaa", "azz"}
	for _, m := range members {
		_ = m
	cDoConn(t, client1, "SADD", "{t}myset", m)
	}
	for _, m := range members {
		_ = m
	cDoConn(t, client1, "SET", fmt.Sprintf("{t}score:%s", m), "100")
	}
	resp := cDoConn(t, client1, "SORT", "{t}myset", "BY", "{t}score:*")
	items := extractBulkStrings(t, resp)
	expected := []string{"a", "aa", "aaa", "azz", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "z"}
	assertStringsEqVar(t, items, expected)
}
