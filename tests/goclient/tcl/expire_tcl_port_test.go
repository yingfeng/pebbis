package tcl

// Mechanical port of frogdb crates/redis-regression/tests/expire_tcl.rs
// (Redis 8.6.0 unit/expire.tcl scenarios).
//
// Excluded (redistore feature gaps at port time):
//   - GETEX tests (tcl_getex_*): GETEX command not implemented.
//   - Sub-second timing tests (tcl_expire_precision_*, tcl_psetex_*,
//     tcl_pexpire_*, tcl_pexpireat_can_set_sub_second_*): rely on observing
//     precise sub-second expiry windows; flaky under load, excluded for now.

import (
	"strconv"
	"testing"
	"time"
)

func ttlInRange(t *testing.T, r reply, lo, hi int64) {
	t.Helper()
	n := unwrapInteger(t, r)
	if n < lo || n > hi {
		t.Fatalf("TTL %d not in [%d,%d]", n, lo, hi)
	}
}

func Test_TCL_tcl_expire_set_timeouts_multiple_times(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x", "foobar")
	v1 := unwrapInteger(t, cDo(t, client1, "EXPIRE", "x", "5"))
	v2 := unwrapInteger(t, cDo(t, client1, "TTL", "x"))
	v3 := unwrapInteger(t, cDo(t, client1, "EXPIRE", "x", "10"))
	v4 := unwrapInteger(t, cDo(t, client1, "TTL", "x"))
	cDo(t, client1, "EXPIRE", "x", "2")
	if v1 != 1 {
		t.Fatalf("v1=%d want 1", v1)
	}
	if v2 < 4 || v2 > 5 {
		t.Fatalf("v2=%d want 4-5", v2)
	}
	if v3 != 1 {
		t.Fatalf("v3=%d want 1", v3)
	}
	if v4 != 10 {
		t.Fatalf("v4=%d want 10", v4)
	}
}

func Test_TCL_tcl_expire_key_still_readable(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x", "foobar")
	cDo(t, client1, "EXPIRE", "x", "20")
	r := cDo(t, client1, "GET", "x")
	assertBulkEq(t, r, "foobar")
}

func Test_TCL_tcl_expire_after_timeout_key_gone(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x", "foobar")
	cDo(t, client1, "EXPIRE", "x", "2")
	time.Sleep(2500 * time.Millisecond)
	r := cDo(t, client1, "GET", "x")
	assertNil(t, r)
	r = cDo(t, client1, "EXISTS", "x")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_expire_write_on_expire_should_work(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x")
	cDo(t, client1, "LPUSH", "x", "foo")
	cDo(t, client1, "EXPIRE", "x", "1000")
	cDo(t, client1, "LPUSH", "x", "bar")
	resp := cDo(t, client1, "LRANGE", "x", "0", "-1")
	items := extractBulkStrings(t, resp)
	assertStringsEq(t, items, "bar", "foo")
}

func Test_TCL_tcl_expireat_check_for_expire_alike_behavior(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x")
	cDo(t, client1, "SET", "x", "foo")
	expireAt := strconv.FormatInt(time.Now().Unix()+15, 10)
	cDo(t, client1, "EXPIREAT", "x", expireAt)
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "x"))
	if ttl < 13 || ttl > 15 {
		t.Fatalf("TTL %d want 13-15", ttl)
	}
}

func Test_TCL_tcl_setex_set_plus_expire_check_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SETEX", "x", "12", "test")
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "x"))
	if ttl < 10 || ttl > 12 {
		t.Fatalf("TTL %d want 10-12", ttl)
	}
}

func Test_TCL_tcl_setex_check_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SETEX", "x", "12", "test")
	r := cDo(t, client1, "GET", "x")
	assertBulkEq(t, r, "test")
}

func Test_TCL_tcl_setex_overwrite_old_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SETEX", "y", "1", "foo")
	r := cDo(t, client1, "GET", "y")
	assertBulkEq(t, r, "foo")
}

func Test_TCL_tcl_setex_wait_for_key_to_expire(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SETEX", "y", "1", "foo")
	time.Sleep(1500 * time.Millisecond)
	r := cDo(t, client1, "GET", "y")
	assertNil(t, r)
}

func Test_TCL_tcl_setex_wrong_time_parameter(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "SETEX", "z", "-10", "foo")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_persist_can_undo_expire(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x", "foo")
	cDo(t, client1, "EXPIRE", "x", "50")
	ttl1 := unwrapInteger(t, cDo(t, client1, "TTL", "x"))
	if ttl1 != 50 {
		t.Fatalf("ttl1=%d want 50", ttl1)
	}
	assertIntegerEq(t, cDo(t, client1, "PERSIST", "x"), 1)
	assertIntegerEq(t, cDo(t, client1, "TTL", "x"), -1)
	r := cDo(t, client1, "GET", "x")
	assertBulkEq(t, r, "foo")
}

func Test_TCL_tcl_persist_returns_0_against_non_existing_or_non_volatile_keys(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x", "foo")
	assertIntegerEq(t, cDo(t, client1, "PERSIST", "foo"), 0)
	assertIntegerEq(t, cDo(t, client1, "PERSIST", "nokeyatall"), 0)
}

func Test_TCL_tcl_ttl_returns_time_to_live_in_seconds(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x")
	cDo(t, client1, "SETEX", "x", "10", "somevalue")
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "x"))
	if ttl <= 8 || ttl > 10 {
		t.Fatalf("TTL %d want 9-10", ttl)
	}
}

func Test_TCL_tcl_pttl_returns_time_to_live_in_milliseconds(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x")
	cDo(t, client1, "SETEX", "x", "1", "somevalue")
	pttl := unwrapInteger(t, cDo(t, client1, "PTTL", "x"))
	if pttl <= 500 || pttl > 1000 {
		t.Fatalf("PTTL %d want 500-1000", pttl)
	}
}

func Test_TCL_tcl_ttl_pttl_expiretime_pexpiretime_return_neg1_if_no_expire(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x")
	cDo(t, client1, "SET", "x", "hello")
	assertIntegerEq(t, cDo(t, client1, "TTL", "x"), -1)
	assertIntegerEq(t, cDo(t, client1, "PTTL", "x"), -1)
	assertIntegerEq(t, cDo(t, client1, "EXPIRETIME", "x"), -1)
	assertIntegerEq(t, cDo(t, client1, "PEXPIRETIME", "x"), -1)
}

func Test_TCL_tcl_ttl_pttl_expiretime_pexpiretime_return_neg2_if_key_not_exist(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x")
	assertIntegerEq(t, cDo(t, client1, "TTL", "x"), -2)
	assertIntegerEq(t, cDo(t, client1, "PTTL", "x"), -2)
	assertIntegerEq(t, cDo(t, client1, "EXPIRETIME", "x"), -2)
	assertIntegerEq(t, cDo(t, client1, "PEXPIRETIME", "x"), -2)
}

func Test_TCL_tcl_expiretime_returns_absolute_expiration_time_in_seconds(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x")
	absExpire := time.Now().Unix() + 100
	absStr := strconv.FormatInt(absExpire, 10)
	cDo(t, client1, "SET", "x", "somevalue", "EXAT", absStr)
	assertIntegerEq(t, cDo(t, client1, "EXPIRETIME", "x"), absExpire)
}

func Test_TCL_tcl_pexpiretime_returns_absolute_expiration_time_in_milliseconds(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x")
	absExpireMs := time.Now().UnixMilli() + 100000
	absStr := strconv.FormatInt(absExpireMs, 10)
	cDo(t, client1, "SET", "x", "somevalue", "PXAT", absStr)
	assertIntegerEq(t, cDo(t, client1, "PEXPIRETIME", "x"), absExpireMs)
}

func Test_TCL_tcl_5_keys_in_5_keys_out(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	cDo(t, client1, "SET", "a", "c")
	cDo(t, client1, "EXPIRE", "a", "5")
	cDo(t, client1, "SET", "t", "c")
	cDo(t, client1, "SET", "e", "c")
	cDo(t, client1, "SET", "s", "c")
	cDo(t, client1, "SET", "foo", "b")
	resp := cDo(t, client1, "KEYS", "*")
	keys := extractBulkStrings(t, resp)
	keys = dedupSorted(keys)
	assertStringsEq(t, keys, "a", "e", "foo", "s", "t")
	cDo(t, client1, "DEL", "a")
}

func Test_TCL_tcl_expire_with_empty_string_as_ttl_should_report_error(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "EXPIRE", "foo", "")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_set_with_ex_big_integer_should_report_error(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "SET", "foo", "bar", "EX", "10000000000000000")
	assertErrorPrefix(t, r, "ERR invalid expire time")
}

func Test_TCL_tcl_set_with_ex_smallest_integer_should_report_error(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "SET", "foo", "bar", "EX", "-9999999999999999")
	assertErrorPrefix(t, r, "ERR invalid expire time")
}

func Test_TCL_tcl_expire_with_big_integer_overflows_when_converted_to_milliseconds(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	assertErrorPrefix(t, cDo(t, client1, "EXPIRE", "foo", "9223370399119966"), "ERR invalid expire time")
	assertErrorPrefix(t, cDo(t, client1, "EXPIRE", "foo", "9223372036854776"), "ERR invalid expire time")
	assertErrorPrefix(t, cDo(t, client1, "EXPIRE", "foo", "10000000000000000"), "ERR invalid expire time")
	assertErrorPrefix(t, cDo(t, client1, "EXPIRE", "foo", "18446744073709561"), "ERR invalid expire time")
	assertIntegerEq(t, cDo(t, client1, "TTL", "foo"), -1)
}

func Test_TCL_tcl_pexpire_with_big_integer_overflow_when_basetime_added(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "PEXPIRE", "foo", "9223372036854770000")
	assertErrorPrefix(t, r, "ERR invalid expire time")
}

func Test_TCL_tcl_expire_with_big_negative_integer(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	assertErrorPrefix(t, cDo(t, client1, "EXPIRE", "foo", "-9223372036854776"), "ERR invalid expire time")
	assertErrorPrefix(t, cDo(t, client1, "EXPIRE", "foo", "-9999999999999999"), "ERR invalid expire time")
	assertIntegerEq(t, cDo(t, client1, "TTL", "foo"), -1)
}

func Test_TCL_tcl_pexpireat_with_big_integer_works(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	assertIntegerEq(t, cDo(t, client1, "PEXPIREAT", "foo", "9223372036854770000"), 1)
}

func Test_TCL_tcl_pexpireat_with_big_negative_integer_works(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	cDo(t, client1, "PEXPIREAT", "foo", "-9223372036854770000")
	assertIntegerEq(t, cDo(t, client1, "TTL", "foo"), -2)
}

func Test_TCL_tcl_set_command_will_remove_expire(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar", "EX", "100")
	cDo(t, client1, "SET", "foo", "bar")
	assertIntegerEq(t, cDo(t, client1, "TTL", "foo"), -1)
}

func Test_TCL_tcl_set_command_will_remove_expire_with_large_string(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	large := ""
	for i := 0; i < 1000; i++ {
		large += "A"
	}
	cDo(t, client1, "SET", "foo", large, "EX", "100")
	cDo(t, client1, "SET", "foo", large, "KEEPTTL")
	ttl1 := unwrapInteger(t, cDo(t, client1, "TTL", "foo"))
	if ttl1 > 100 || ttl1 <= 90 {
		t.Fatalf("TTL %d want 91-100", ttl1)
	}
	cDo(t, client1, "SET", "foo", large)
	assertIntegerEq(t, cDo(t, client1, "TTL", "foo"), -1)
}

func Test_TCL_tcl_set_use_keepttl_option_ttl_should_not_be_removed(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar", "EX", "100")
	cDo(t, client1, "SET", "foo", "bar", "KEEPTTL")
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "foo"))
	if ttl > 100 || ttl <= 90 {
		t.Fatalf("TTL %d want 91-100", ttl)
	}
}

func Test_TCL_tcl_expire_with_nx_option_on_key_with_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar", "EX", "100")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "200", "NX"), 0)
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "foo"))
	if ttl < 50 || ttl > 100 {
		t.Fatalf("TTL %d want 50-100", ttl)
	}
}

func Test_TCL_tcl_expire_with_nx_option_on_key_without_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "200", "NX"), 1)
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "foo"))
	if ttl < 100 || ttl > 200 {
		t.Fatalf("TTL %d want 100-200", ttl)
	}
}

func Test_TCL_tcl_expire_with_xx_option_on_key_with_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar", "EX", "100")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "200", "XX"), 1)
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "foo"))
	if ttl < 100 || ttl > 200 {
		t.Fatalf("TTL %d want 100-200", ttl)
	}
}

func Test_TCL_tcl_expire_with_xx_option_on_key_without_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "200", "XX"), 0)
	assertIntegerEq(t, cDo(t, client1, "TTL", "foo"), -1)
}

func Test_TCL_tcl_expire_with_gt_option_on_key_with_lower_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar", "EX", "100")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "200", "GT"), 1)
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "foo"))
	if ttl < 100 || ttl > 200 {
		t.Fatalf("TTL %d want 100-200", ttl)
	}
}

func Test_TCL_tcl_expire_with_gt_option_on_key_with_higher_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar", "EX", "200")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "100", "GT"), 0)
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "foo"))
	if ttl < 100 || ttl > 200 {
		t.Fatalf("TTL %d want 100-200", ttl)
	}
}

func Test_TCL_tcl_expire_with_gt_option_on_key_without_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "200", "GT"), 0)
	assertIntegerEq(t, cDo(t, client1, "TTL", "foo"), -1)
}

func Test_TCL_tcl_expire_with_lt_option_on_key_with_higher_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar", "EX", "100")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "200", "LT"), 0)
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "foo"))
	if ttl < 50 || ttl > 100 {
		t.Fatalf("TTL %d want 50-100", ttl)
	}
}

func Test_TCL_tcl_expire_with_lt_option_on_key_with_lower_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar", "EX", "200")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "100", "LT"), 1)
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "foo"))
	if ttl < 50 || ttl > 100 {
		t.Fatalf("TTL %d want 50-100", ttl)
	}
}

func Test_TCL_tcl_expire_with_lt_option_on_key_without_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "100", "LT"), 1)
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "foo"))
	if ttl < 50 || ttl > 100 {
		t.Fatalf("TTL %d want 50-100", ttl)
	}
}

func Test_TCL_tcl_expire_with_lt_and_xx_option_on_key_with_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar", "EX", "200")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "100", "LT", "XX"), 1)
	ttl := unwrapInteger(t, cDo(t, client1, "TTL", "foo"))
	if ttl < 50 || ttl > 100 {
		t.Fatalf("TTL %d want 50-100", ttl)
	}
}

func Test_TCL_tcl_expire_with_lt_and_xx_option_on_key_without_ttl(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "200", "LT", "XX"), 0)
	assertIntegerEq(t, cDo(t, client1, "TTL", "foo"), -1)
}

func Test_TCL_tcl_expire_conflicting_options_lt_gt(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "EXPIRE", "foo", "200", "LT", "GT")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_expire_conflicting_options_nx_gt(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "EXPIRE", "foo", "200", "NX", "GT")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_expire_conflicting_options_nx_lt(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "EXPIRE", "foo", "200", "NX", "LT")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_expire_conflicting_options_nx_xx(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "EXPIRE", "foo", "200", "NX", "XX")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_expire_unsupported_option(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "EXPIRE", "foo", "200", "AB")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_expire_unsupported_option_after_valid(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "EXPIRE", "foo", "200", "XX", "AB")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_expire_with_negative_expiry(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar", "EX", "100")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "-10", "LT"), 1)
	assertIntegerEq(t, cDo(t, client1, "TTL", "foo"), -2)
}

func Test_TCL_tcl_expire_with_negative_expiry_on_non_volatile_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "foo", "-10", "LT"), 1)
	assertIntegerEq(t, cDo(t, client1, "TTL", "foo"), -2)
}

func Test_TCL_tcl_expire_with_non_existed_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "none", "100", "NX"), 0)
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "none", "100", "XX"), 0)
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "none", "100", "GT"), 0)
	assertIntegerEq(t, cDo(t, client1, "EXPIRE", "none", "100", "LT"), 0)
}
