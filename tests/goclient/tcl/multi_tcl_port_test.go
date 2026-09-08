package tcl

// Mechanical port of frogdb crates/redis-regression/tests/multi_tcl.rs
// (Redis 8.6.0 unit scenarios).
import (
	"testing"
	"time"
)
func Test_TCL_tcl_multi_exec_basics(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mylist")
	cDoConn(t, client1, "RPUSH", "mylist", "a")
	cDoConn(t, client1, "RPUSH", "mylist", "b")
	cDoConn(t, client1, "RPUSH", "mylist", "c")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "LRANGE", "mylist", "0", "-1"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 2 { t.Fatalf("len mismatch: got %d want 2", len(results)) }
	items := extractBulkStrings(t, results[0])
	assertStringsEq(t, items, "a", "b", "c")
	if results[1].val != "PONG" { t.Fatalf("match failed: got %v", results[1].val) }
}

func Test_TCL_tcl_discard(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "mylist")
	cDoConn(t, client1, "RPUSH", "mylist", "a")
	cDoConn(t, client1, "RPUSH", "mylist", "b")
	cDoConn(t, client1, "RPUSH", "mylist", "c")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "DEL", "mylist"))
	assertOK(t, cDoConn(t, client1, "DISCARD"))
	resp := cDoConn(t, client1, "LRANGE", "mylist", "0", "-1")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "a", "b", "c")
}

func Test_TCL_tcl_nested_multi_not_allowed(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "MULTI"))
	resp := cDoConn(t, client1, "MULTI")
	assertErrorPrefix(t, resp, "ERR")
	cDoConn(t, client1, "EXEC")
}

func Test_TCL_tcl_multi_where_commands_alter_argc_argv(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SADD", "myset", "a")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "SPOP", "myset"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	assertBulkEq(t, results[0], "a")
	cDoConn(t, client1, "EXISTS", "myset")
}

func Test_TCL_tcl_watch_inside_multi_not_allowed(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "MULTI"))
	resp := cDoConn(t, client1, "WATCH", "x")
	assertErrorPrefix(t, resp, "ERR")
	cDoConn(t, client1, "EXEC")
}

func Test_TCL_tcl_exec_fails_with_queuing_errors(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "foo1{t}", "foo2{t}")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "SET", "foo1{t}", "bar1"))
	resp := cDoConn(t, client1, "NON-EXISTING-COMMAND")
	assertErrorPrefix(t, resp, "ERR")
	assertQueued(t, cDoConn(t, client1, "SET", "foo2{t}", "bar2"))
	exec_resp := cDoConn(t, client1, "EXEC")
	assertErrorPrefix(t, exec_resp, "EXECABORT")
	cDoConn(t, client1, "EXISTS", "foo1{t}")
	cDoConn(t, client1, "EXISTS", "foo2{t}")
}

func Test_TCL_tcl_if_exec_aborts_client_multi_state_cleared(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "foo1{t}", "foo2{t}")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "SET", "foo1{t}", "bar1"))
	cDoConn(t, client1, "NON-EXISTING-COMMAND")
	assertQueued(t, cDoConn(t, client1, "SET", "foo2{t}", "bar2"))
	exec_resp := cDoConn(t, client1, "EXEC")
	assertErrorPrefix(t, exec_resp, "EXECABORT")
	resp := cDoConn(t, client1, "PING")
	if resp.val != "PONG" { t.Fatalf("match failed: got %v", resp.val) }
}

func Test_TCL_tcl_exec_works_on_watched_key_not_modified(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "WATCH", "x{t}", "y{t}", "z{t}"))
	assertOK(t, cDoConn(t, client1, "WATCH", "k{t}"))
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	if results[0].val != "PONG" { t.Fatalf("match failed: got %v", results[0].val) }
}

func Test_TCL_tcl_exec_fail_on_watched_key_modified_1_of_1(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "x", "30")
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	cDoConn(t, client1, "SET", "x", "40")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	assertExecAborted(t, resp)
}

func Test_TCL_tcl_exec_fail_on_watched_key_modified_1_of_5(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "x{t}", "30")
	assertOK(t, cDoConn(t, client1, "WATCH", "a{t}", "b{t}", "x{t}", "k{t}", "z{t}"))
	// PORT-TODO: );
	cDoConn(t, client1, "SET", "x{t}", "40")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	assertExecAborted(t, resp)
}

func Test_TCL_tcl_exec_fail_on_watched_key_modified_by_sort_store_empty(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "FLUSHDB")
	cDoConn(t, client1, "LPUSH", "foo{t}", "bar")
	assertOK(t, cDoConn(t, client1, "WATCH", "foo{t}"))
	cDoConn(t, client1, "SORT", "emptylist{t}", "STORE", "foo{t}")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	assertExecAborted(t, resp)
}

func Test_TCL_tcl_after_successful_exec_key_no_longer_watched(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "x", "30")
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	if results[0].val != "PONG" { t.Fatalf("match failed: got %v", results[0].val) }
	cDoConn(t, client1, "SET", "x", "40")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp = cDoConn(t, client1, "EXEC")
	results = unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	if results[0].val != "PONG" { t.Fatalf("match failed: got %v", results[0].val) }
}

func Test_TCL_tcl_after_failed_exec_key_no_longer_watched(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "x", "30")
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	cDoConn(t, client1, "SET", "x", "40")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	assertExecAborted(t, resp)
	cDoConn(t, client1, "SET", "x", "40")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp = cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	if results[0].val != "PONG" { t.Fatalf("match failed: got %v", results[0].val) }
}

func Test_TCL_tcl_it_is_possible_to_unwatch(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "x", "30")
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	cDoConn(t, client1, "SET", "x", "40")
	assertOK(t, cDoConn(t, client1, "UNWATCH"))
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	if results[0].val != "PONG" { t.Fatalf("match failed: got %v", results[0].val) }
}

func Test_TCL_tcl_unwatch_when_nothing_watched(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "UNWATCH"))
}

func Test_TCL_tcl_flushall_touches_watched_keys(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "x", "30")
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	cDoConn(t, client1, "FLUSHALL")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	assertExecAborted(t, resp)
}

func Test_TCL_tcl_flushall_does_not_touch_non_affected_keys(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "x")
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	cDoConn(t, client1, "FLUSHALL")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	if results[0].val != "PONG" { t.Fatalf("match failed: got %v", results[0].val) }
}

func Test_TCL_tcl_flushdb_touches_watched_keys(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "SET", "x", "30")
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	cDoConn(t, client1, "FLUSHDB")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	assertExecAborted(t, resp)
}

func Test_TCL_tcl_flushdb_does_not_touch_non_affected_keys(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "x")
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	cDoConn(t, client1, "FLUSHDB")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	if results[0].val != "PONG" { t.Fatalf("match failed: got %v", results[0].val) }
}

func Test_TCL_tcl_watch_considers_expire_on_watched_key(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "DEL", "x")
	cDoConn(t, client1, "SET", "x", "foo")
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	cDoConn(t, client1, "EXPIRE", "x", "10")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	assertExecAborted(t, resp)
}

func Test_TCL_tcl_watch_considers_touched_expired_keys(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "FLUSHALL")
	cDoConn(t, client1, "DEL", "x")
	cDoConn(t, client1, "SET", "x", "foo")
	cDoConn(t, client1, "EXPIRE", "x", "1")
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	time.Sleep(1500 * time.Millisecond)
	cDoConn(t, client1, "DBSIZE")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	assertExecAborted(t, resp)
}

func Test_TCL_tcl_discard_clears_watch_dirty_flag(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	cDoConn(t, client1, "SET", "x", "10")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertOK(t, cDoConn(t, client1, "DISCARD"))
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "INCR", "x"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	assertIntegerEq(t, results[0], 11)
}

func Test_TCL_tcl_discard_unwatches_all_keys(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	cDoConn(t, client1, "SET", "x", "10")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertOK(t, cDoConn(t, client1, "DISCARD"))
	cDoConn(t, client1, "SET", "x", "10")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "INCR", "x"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	assertIntegerEq(t, results[0], 11)
}

func Test_TCL_tcl_blocking_commands_ignore_timeout_in_multi(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "XGROUP", "CREATE", "s{t}", "g", "$", "MKSTREAM")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "BLPOP", "empty_list{t}", "0"))
	assertQueued(t, cDoConn(t, client1, "BRPOP", "empty_list{t}", "0"))
	assertQueued(t, cDoConn(t, client1, "BRPOPLPUSH", "empty_list1{t}", "empty_list2{t}", "0"))
	// PORT-TODO: );
	assertQueued(t, cDoConn(t, client1, "BLMOVE", "empty_list1{t}", "empty_list2{t}", "LEFT", "LEFT", "0"))
	// PORT-TODO: );
	assertQueued(t, cDoConn(t, client1, "BZPOPMIN", "empty_zset{t}", "0"))
	assertQueued(t, cDoConn(t, client1, "BZPOPMAX", "empty_zset{t}", "0"))
	assertQueued(t, cDoConn(t, client1, "XREAD", "BLOCK", "0", "STREAMS", "s{t}", "$"))
	// PORT-TODO: );
	assertQueued(t, cDoConn(t, client1, "XREADGROUP", "GROUP", "g", "c", "BLOCK", "0", "STREAMS", "s{t}", ">"))
	// PORT-TODO: );
	assertQueued(t, cDoConn(t, client1, "BLMPOP", "0", "1", "empty_list{t}", "LEFT"))
	// PORT-TODO: );
	assertQueued(t, cDoConn(t, client1, "BZMPOP", "0", "1", "empty_zset{t}", "MIN"))
	// PORT-TODO: );
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 10 { t.Fatalf("len mismatch: got %d want 10", len(results)) }
	for i, r := range results {
		_ = i
		_ = r
	// PORT-TODO: assert!(
	// PORT-TODO: matches!(r, Response::Bulk(None) | Response::Null),
	// PORT-TODO: "expected nil for blocking command {i}, got {r:?}"
	// PORT-TODO: );
	}
}

func Test_TCL_tcl_blmpop_bzmpop_return_data_immediately_in_multi(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "RPUSH", "list{t}", "a", "b")
	cDoConn(t, client1, "ZADD", "zset{t}", "1", "one", "2", "two")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "BLMPOP", "0", "1", "list{t}", "LEFT"))
	// PORT-TODO: );
	assertQueued(t, cDoConn(t, client1, "BZMPOP", "0", "1", "zset{t}", "MIN"))
	// PORT-TODO: );
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 2 { t.Fatalf("len mismatch: got %d want 2", len(results)) }
	blmpop := unwrapArray(t, results[0])
	if len(blmpop) != 2 { t.Fatalf("len mismatch: got %d want 2", len(blmpop)) }
	assertBulkEq(t, blmpop[0], "list{t}")
	popped := unwrapArray(t, blmpop[1])
	if len(popped) != 1 { t.Fatalf("len mismatch: got %d want 1", len(popped)) }
	assertBulkEq(t, popped[0], "a")
	bzmpop := unwrapArray(t, results[1])
	if len(bzmpop) != 2 { t.Fatalf("len mismatch: got %d want 2", len(bzmpop)) }
	assertBulkEq(t, bzmpop[0], "zset{t}")
	popped = unwrapArray(t, bzmpop[1])
	if len(popped) != 1 { t.Fatalf("len mismatch: got %d want 1", len(popped)) }
	member := unwrapArray(t, popped[0])
	assertBulkEq(t, member[0], "one")
	assertBulkEq(t, member[1], "1")
}

func Test_TCL_tcl_flushall_while_watching_several_keys_one_client(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	cDoConn(t, client1, "FLUSHALL")
	cDoConn(t, client1, "MSET", "a{t}", "a", "b{t}", "b")
	assertOK(t, cDoConn(t, client1, "WATCH", "b{t}", "a{t}"))
	cDoConn(t, client1, "FLUSHALL")
	resp := cDoConn(t, client1, "PING")
	if resp.val != "PONG" { t.Fatalf("match failed: got %v", resp.val) }
}

func Test_TCL_tcl_exec_with_at_least_one_use_memory_command_should_fail(t *testing.T) {
	addr := startServer(t)
	// PORT-TODO: num_shards: Some(1),
	// PORT-TODO: ..Default::default()
	// PORT-TODO: })
	// PORT-TODO: .await;
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "x", "hello"))
	assertOK(t, cDoConn(t, client1, "CONFIG", "SET", "maxmemory", "1"))
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "SET", "y", "world"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	assertErrorPrefix(t, results[0], "OOM")
	assertOK(t, cDoConn(t, client1, "CONFIG", "SET", "maxmemory", "0"))
}

func Test_TCL_tcl_exec_with_only_read_commands_should_not_be_rejected_when_oom(t *testing.T) {
	addr := startServer(t)
	// PORT-TODO: num_shards: Some(1),
	// PORT-TODO: ..Default::default()
	// PORT-TODO: })
	// PORT-TODO: .await;
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "x", "hello"))
	assertOK(t, cDoConn(t, client1, "CONFIG", "SET", "maxmemory", "1"))
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "GET", "x"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	assertBulkEq(t, results[0], "hello")
	assertOK(t, cDoConn(t, client1, "CONFIG", "SET", "maxmemory", "0"))
}

func Test_TCL_tcl_watch_stale_keys_should_not_fail_exec(t *testing.T) {
	addr := startServer(t)
	// PORT-TODO: num_shards: Some(1),
	// PORT-TODO: ..Default::default()
	// PORT-TODO: })
	// PORT-TODO: .await;
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "x", "foo"))
	cDoConn(t, client1, "PEXPIRE", "x", "100")
	assertOK(t, cDoConn(t, client1, "DEBUG", "SET-ACTIVE-EXPIRE", "0"))
	time.Sleep(200 * time.Millisecond)
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	if results[0].val != "PONG" { t.Fatalf("match failed: got %v", results[0].val) }
	assertOK(t, cDoConn(t, client1, "DEBUG", "SET-ACTIVE-EXPIRE", "1"))
}

func Test_TCL_tcl_delete_watched_stale_keys_should_not_fail_exec(t *testing.T) {
	addr := startServer(t)
	// PORT-TODO: num_shards: Some(1),
	// PORT-TODO: ..Default::default()
	// PORT-TODO: })
	// PORT-TODO: .await;
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "x", "foo"))
	cDoConn(t, client1, "PEXPIRE", "x", "100")
	assertOK(t, cDoConn(t, client1, "DEBUG", "SET-ACTIVE-EXPIRE", "0"))
	time.Sleep(200 * time.Millisecond)
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	del_resp := cDoConn(t, client1, "DEL", "x")
	assertIntegerEq(t, del_resp, 0)
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	if results[0].val != "PONG" { t.Fatalf("match failed: got %v", results[0].val) }
	assertOK(t, cDoConn(t, client1, "DEBUG", "SET-ACTIVE-EXPIRE", "1"))
}

func Test_TCL_tcl_flushdb_while_watching_stale_keys_should_not_fail_exec(t *testing.T) {
	addr := startServer(t)
	// PORT-TODO: num_shards: Some(1),
	// PORT-TODO: ..Default::default()
	// PORT-TODO: })
	// PORT-TODO: .await;
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "SET", "x", "foo"))
	cDoConn(t, client1, "PEXPIRE", "x", "100")
	assertOK(t, cDoConn(t, client1, "DEBUG", "SET-ACTIVE-EXPIRE", "0"))
	time.Sleep(200 * time.Millisecond)
	assertOK(t, cDoConn(t, client1, "WATCH", "x"))
	cDoConn(t, client1, "FLUSHDB")
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "PING"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 1 { t.Fatalf("len mismatch: got %d want 1", len(results)) }
	if results[0].val != "PONG" { t.Fatalf("match failed: got %v", results[0].val) }
	assertOK(t, cDoConn(t, client1, "DEBUG", "SET-ACTIVE-EXPIRE", "1"))
}

func Test_TCL_tcl_multi_and_script_timeout(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "CONFIG", "SET", "lua-time-limit", "1"))
	// PORT-TODO: );
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertOK(t, cDoConn(t, client1, "DISCARD"))
	assertOK(t, cDoConn(t, client1, "CONFIG", "SET", "lua-time-limit", "5000"))
	// PORT-TODO: );
}

func Test_TCL_tcl_exec_and_script_timeout(t *testing.T) {
	t.Skip("infinite-loop script timeout not supported")
}

func Test_TCL_tcl_just_exec_and_script_timeout(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "CONFIG", "SET", "lua-time-limit", "1"))
	// PORT-TODO: );
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "SET", "{k}x", "hello"))
	assertQueued(t, cDoConn(t, client1, "GET", "{k}x"))
	r := cDoConn(t, client1, "EXEC")
	items := unwrapArray(t, r)
	if len(items) != 2 { t.Fatalf("len mismatch: got %d want 2", len(items)) }
	assertOK(t, items[0])
	assertBulkEq(t, items[1], "hello")
	// PORT-TODO: }
	// PORT-TODO: other => panic!("expected array, got: {other:?}"),
	// PORT-TODO: }
	assertOK(t, cDoConn(t, client1, "CONFIG", "SET", "lua-time-limit", "5000"))
	// PORT-TODO: );
}

func Test_TCL_tcl_multi_with_config_error(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	assertOK(t, cDoConn(t, client1, "MULTI"))
	assertQueued(t, cDoConn(t, client1, "CONFIG", "SET", "nonexistent-config-param", "value"))
	// PORT-TODO: );
	assertQueued(t, cDoConn(t, client1, "SET", "x", "hello"))
	resp := cDoConn(t, client1, "EXEC")
	results := unwrapArray(t, resp)
	if len(results) != 2 { t.Fatalf("len mismatch: got %d want 2", len(results)) }
	assertErrorPrefix(t, results[0], "ERR")
	assertOK(t, results[1])
	cDoConn(t, client1, "GET", "x")
}
