package tcl

// Mechanical port of frogdb crates/redis-regression/tests/keyspace_tcl.rs
// (Redis 8.6.0 unit/keyspace.tcl scenarios).

import (
	"strings"
	"testing"
)

func Test_TCL_tcl_del_single_item(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x", "foo")
	r := cDo(t, client1, "GET", "x")
	assertBulkEq(t, r, "foo")
	cDo(t, client1, "DEL", "x")
	r = cDo(t, client1, "GET", "x")
	assertNil(t, r)
}

func Test_TCL_tcl_vararg_del(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "{foo}1", "a")
	cDo(t, client1, "SET", "{foo}2", "b")
	cDo(t, client1, "SET", "{foo}3", "c")
	assertIntegerEq(t, cDo(t, client1, "DEL", "{foo}1", "{foo}2", "{foo}3", "{foo}4"), 3)
	r := cDo(t, client1, "GET", "{foo}1")
	assertNil(t, r)
	r = cDo(t, client1, "GET", "{foo}2")
	assertNil(t, r)
	r = cDo(t, client1, "GET", "{foo}3")
	assertNil(t, r)
}

func Test_TCL_tcl_untagged_multi_key_commands(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "MSET", "{foo}1", "a", "{foo}2", "b", "{foo}3", "c")
	resp := cDo(t, client1, "MGET", "{foo}1", "{foo}2", "{foo}3", "{foo}4")
	vals := extractBulkStrings(t, resp)
	assertStringsEq(t, vals, "a", "b", "c")
	assertIntegerEq(t, cDo(t, client1, "DEL", "{foo}1", "{foo}2", "{foo}3", "{foo}4"), 3)
}

func Test_TCL_tcl_keys_with_pattern(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	for _, key := range []string{"key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"} {
		cDo(t, client1, "SET", key, "hello")
	}
	resp := cDo(t, client1, "KEYS", "foo*")
	keys := dedupSorted(extractBulkStrings(t, resp))
	assertStringsEq(t, keys, "foo_a", "foo_b", "foo_c")
}

func Test_TCL_tcl_keys_all(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	for _, key := range []string{"key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"} {
		cDo(t, client1, "SET", key, "hello")
	}
	resp := cDo(t, client1, "KEYS", "*")
	keys := dedupSorted(extractBulkStrings(t, resp))
	assertStringsEq(t, keys, "foo_a", "foo_b", "foo_c", "key_x", "key_y", "key_z")
}

func Test_TCL_tcl_dbsize(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	for _, key := range []string{"key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"} {
		cDo(t, client1, "SET", key, "hello")
	}
	assertIntegerEq(t, cDo(t, client1, "DBSIZE"), 6)
}

func Test_TCL_tcl_keys_with_hashtag(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	for _, key := range []string{"{a}x", "{a}y", "{a}z", "{b}a", "{b}b", "{b}c"} {
		cDo(t, client1, "SET", key, "hello")
	}
	resp := cDo(t, client1, "KEYS", "{a}*")
	keys := dedupSorted(extractBulkStrings(t, resp))
	assertStringsEq(t, keys, "{a}x", "{a}y", "{a}z")
	resp = cDo(t, client1, "KEYS", "*{b}*")
	keys = dedupSorted(extractBulkStrings(t, resp))
	assertStringsEq(t, keys, "{b}a", "{b}b", "{b}c")
}

func Test_TCL_tcl_del_all_keys(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	for _, key := range []string{"key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"} {
		cDo(t, client1, "SET", key, "hello")
	}
	resp := cDo(t, client1, "KEYS", "*")
	keys := extractBulkStrings(t, resp)
	for _, key := range keys {
		cDo(t, client1, "DEL", key)
	}
	assertIntegerEq(t, cDo(t, client1, "DBSIZE"), 0)
}

func Test_TCL_tcl_exists(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "newkey", "test")
	assertIntegerEq(t, cDo(t, client1, "EXISTS", "newkey"), 1)
	cDo(t, client1, "DEL", "newkey")
	assertIntegerEq(t, cDo(t, client1, "EXISTS", "newkey"), 0)
}

func Test_TCL_tcl_zero_length_value_set_get_exists(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "emptykey", "")
	r := cDo(t, client1, "GET", "emptykey")
	assertBulkEq(t, r, "")
	assertIntegerEq(t, cDo(t, client1, "EXISTS", "emptykey"), 1)
	cDo(t, client1, "DEL", "emptykey")
	assertIntegerEq(t, cDo(t, client1, "EXISTS", "emptykey"), 0)
}

func Test_TCL_tcl_non_existing_command(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "FOOBAREDCOMMAND")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_rename_basic_usage(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "{my}key", "hello")
	assertOK(t, cDo(t, client1, "RENAME", "{my}key", "{my}key1"))
	assertOK(t, cDo(t, client1, "RENAME", "{my}key1", "{my}key2"))
	r := cDo(t, client1, "GET", "{my}key2")
	assertBulkEq(t, r, "hello")
}

func Test_TCL_tcl_rename_source_key_no_longer_exists(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "hello")
	cDo(t, client1, "RENAME", "mykey", "mykey2")
	assertIntegerEq(t, cDo(t, client1, "EXISTS", "mykey"), 0)
}

func Test_TCL_tcl_rename_against_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "a")
	cDo(t, client1, "SET", "mykey2", "b")
	cDo(t, client1, "RENAME", "mykey2", "mykey")
	r := cDo(t, client1, "GET", "mykey")
	assertBulkEq(t, r, "b")
	assertIntegerEq(t, cDo(t, client1, "EXISTS", "mykey2"), 0)
}

func Test_TCL_tcl_renamenx_basic_usage(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mykey")
	cDo(t, client1, "DEL", "mykey2")
	cDo(t, client1, "SET", "mykey", "foobar")
	assertIntegerEq(t, cDo(t, client1, "RENAMENX", "mykey", "mykey2"), 1)
	r := cDo(t, client1, "GET", "mykey2")
	assertBulkEq(t, r, "foobar")
	assertIntegerEq(t, cDo(t, client1, "EXISTS", "mykey"), 0)
}

func Test_TCL_tcl_renamenx_against_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "foo")
	cDo(t, client1, "SET", "mykey2", "bar")
	assertIntegerEq(t, cDo(t, client1, "RENAMENX", "mykey", "mykey2"), 0)
}

func Test_TCL_tcl_renamenx_against_existing_key_values_preserved(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "foo")
	cDo(t, client1, "SET", "mykey2", "bar")
	cDo(t, client1, "RENAMENX", "mykey", "mykey2")
	r := cDo(t, client1, "GET", "mykey")
	assertBulkEq(t, r, "foo")
	r = cDo(t, client1, "GET", "mykey2")
	assertBulkEq(t, r, "bar")
}

func Test_TCL_tcl_rename_non_existing_source_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "RENAME", "{t}nokey", "{t}foobar")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_rename_source_and_dest_same_existing(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "foo")
	assertOK(t, cDo(t, client1, "RENAME", "mykey", "mykey"))
}

func Test_TCL_tcl_renamenx_source_and_dest_same_existing(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "foo")
	assertIntegerEq(t, cDo(t, client1, "RENAMENX", "mykey", "mykey"), 0)
}

func Test_TCL_tcl_rename_source_and_dest_same_non_existing(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mykey")
	r := cDo(t, client1, "RENAME", "mykey", "mykey")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_rename_volatile_key_moves_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mykey", "mykey2")
	cDo(t, client1, "SET", "mykey", "foo")
	cDo(t, client1, "EXPIRE", "mykey", "100")
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "mykey"))
	if ttl <= 95 || ttl > 100 {
		t.Fatalf("TTL %d want 96-100", ttl)
	}
	cDo(t, client1, "RENAME", "mykey", "mykey2")
	ttl2 := unwrapInteger(t, cDo(t, client1, "TTL", "mykey2"))
	if ttl2 <= 95 || ttl2 > 100 {
		t.Fatalf("TTL2 %d want 96-100", ttl2)
	}
}

func Test_TCL_tcl_rename_volatile_key_should_not_inherit_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mykey", "mykey2")
	cDo(t, client1, "SET", "mykey", "foo")
	cDo(t, client1, "SET", "mykey2", "bar")
	cDo(t, client1, "EXPIRE", "mykey2", "100")
	ttlMykey := unwrapInteger(t, cDo(t, client1, "TTL", "mykey"))
	ttlMykey2 := unwrapInteger(t, cDo(t, client1, "TTL", "mykey2"))
	if ttlMykey != -1 {
		t.Fatalf("mykey TTL %d want -1", ttlMykey)
	}
	if ttlMykey2 <= 0 {
		t.Fatalf("mykey2 TTL %d want >0", ttlMykey2)
	}
	cDo(t, client1, "RENAME", "mykey", "mykey2")
	assertIntegerEq(t, cDo(t, client1, "TTL", "mykey2"), -1)
}

func Test_TCL_tcl_del_all_keys_db0(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "a", "1")
	cDo(t, client1, "SET", "b", "2")
	resp := cDo(t, client1, "KEYS", "*")
	keys := extractBulkStrings(t, resp)
	for _, key := range keys {
		cDo(t, client1, "DEL", key)
	}
	assertIntegerEq(t, cDo(t, client1, "DBSIZE"), 0)
}

func Test_TCL_tcl_copy_basic_usage_for_string(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "{my}key", "foobar")
	assertIntegerEq(t, cDo(t, client1, "COPY", "{my}key", "{my}newkey"), 1)
	r := cDo(t, client1, "GET", "{my}newkey")
	assertBulkEq(t, r, "foobar")
}

func Test_TCL_tcl_copy_string_does_not_copy_to_non_integer_db(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "{my}key", "foobar")
	r := cDo(t, client1, "COPY", "{my}key", "{my}newkey", "DB", "notanumber")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_copy_key_expire_metadata(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "{my}key", "foobar", "EX", "100")
	cDo(t, client1, "COPY", "{my}key", "{my}newkey", "REPLACE")
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "{my}newkey"))
	if ttl <= 0 || ttl > 100 {
		t.Fatalf("TTL %d want 1-100", ttl)
	}
	r := cDo(t, client1, "GET", "{my}newkey")
	assertBulkEq(t, r, "foobar")
}

func Test_TCL_tcl_copy_does_not_create_expire_if_none(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "{my}key", "foobar")
	assertIntegerEq(t, cDo(t, client1, "TTL", "{my}key"), -1)
	cDo(t, client1, "COPY", "{my}key", "{my}newkey", "REPLACE")
	assertIntegerEq(t, cDo(t, client1, "TTL", "{my}newkey"), -1)
	r := cDo(t, client1, "GET", "{my}newkey")
	assertBulkEq(t, r, "foobar")
}

func Test_TCL_tcl_copy_does_not_replace_without_option(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "{my}key", "foobar")
	cDo(t, client1, "SET", "{my}newkey", "existing")
	assertIntegerEq(t, cDo(t, client1, "COPY", "{my}key", "{my}newkey"), 0)
	r := cDo(t, client1, "GET", "{my}newkey")
	assertBulkEq(t, r, "existing")
}

func Test_TCL_tcl_copy_replaces_with_replace_option(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "{my}key", "foobar")
	cDo(t, client1, "SET", "{my}newkey", "existing")
	assertIntegerEq(t, cDo(t, client1, "COPY", "{my}key", "{my}newkey", "REPLACE"), 1)
	r := cDo(t, client1, "GET", "{my}newkey")
	assertBulkEq(t, r, "foobar")
}

func Test_TCL_tcl_randomkey(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	cDo(t, client1, "SET", "foo", "x")
	cDo(t, client1, "SET", "bar", "y")
	fooSeen, barSeen := false, false
	for i := 0; i < 100; i++ {
		k := parseBulkString(t, cDo(t, client1, "RANDOMKEY"))
		if k == "foo" {
			fooSeen = true
		}
		if k == "bar" {
			barSeen = true
		}
	}
	if !fooSeen {
		t.Fatalf("expected to see key 'foo'")
	}
	if !barSeen {
		t.Fatalf("expected to see key 'bar'")
	}
}

func Test_TCL_tcl_randomkey_against_empty_db(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	r := cDo(t, client1, "RANDOMKEY")
	assertNil(t, r)
}

func Test_TCL_tcl_randomkey_regression_1(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	cDo(t, client1, "SET", "x", "10")
	cDo(t, client1, "DEL", "x")
	r := cDo(t, client1, "RANDOMKEY")
	assertNil(t, r)
}

func Test_TCL_tcl_keys_star_twice_with_long_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	longKey := "dlskeriewrioeuwqoirueioqwrueoqwrueqw"
	cDo(t, client1, "SET", longKey, "test")
	cDo(t, client1, "KEYS", "*")
	resp := cDo(t, client1, "KEYS", "*")
	keys := extractBulkStrings(t, resp)
	assertStringsEq(t, keys, longKey)
}

func Test_TCL_tcl_regression_pattern_matching_long_nested_loops(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	key := strings.Repeat("a", 40)
	cDo(t, client1, "SET", key, "1")
	pattern := strings.Repeat("a*a*", 20) + "b"
	resp := cDo(t, client1, "KEYS", pattern)
	keys := extractBulkStrings(t, resp)
	if len(keys) != 0 {
		t.Fatalf("expected no keys, got %v", keys)
	}
}

func Test_TCL_tcl_regression_pattern_matching_very_long_nested_loops(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	key := strings.Repeat("a", 50000)
	cDo(t, client1, "SET", key, "1")
	pattern := strings.Repeat("*?", 50000)
	resp := cDo(t, client1, "KEYS", pattern)
	keys := extractBulkStrings(t, resp)
	if len(keys) != 0 {
		t.Fatalf("expected no keys, got %v", keys)
	}
}
