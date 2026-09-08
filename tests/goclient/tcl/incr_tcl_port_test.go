package tcl

// Mechanical port of frogdb crates/redis-regression/tests/incr_tcl.rs
// (Redis 8.6.0 unit/type/incr.tcl scenarios).

import (
	"testing"
)

func Test_TCL_tcl_incr_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "INCR", "novar")
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "GET", "novar")
	assertBulkEq(t, r, "1")
}

func Test_TCL_tcl_incr_against_key_created_by_incr_itself(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "INCR", "novar")
	r := cDo(t, client1, "INCR", "novar")
	assertIntegerEq(t, r, 2)
}

func Test_TCL_tcl_decr_against_key_created_by_incr(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "INCR", "novar")
	r := cDo(t, client1, "DECR", "novar")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_decr_against_key_not_exist_and_incr(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "novar_not_exist")
	r := cDo(t, client1, "DECR", "novar_not_exist")
	assertIntegerEq(t, r, -1)
	r = cDo(t, client1, "INCR", "novar_not_exist")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_incr_against_key_originally_set_with_set(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "100")
	r := cDo(t, client1, "INCR", "novar")
	assertIntegerEq(t, r, 101)
}

func Test_TCL_tcl_incr_over_32bit_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "17179869184")
	r := cDo(t, client1, "INCR", "novar")
	assertIntegerEq(t, r, 17179869185)
}

func Test_TCL_tcl_incrby_over_32bit_value_with_over_32bit_increment(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "17179869184")
	r := cDo(t, client1, "INCRBY", "novar", "17179869184")
	assertIntegerEq(t, r, 34359738368)
}

func Test_TCL_tcl_incr_does_not_use_shared_objects(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "-1")
	cDo(t, client1, "INCR", "foo")
	r := cDo(t, client1, "OBJECT", "REFCOUNT", "foo")
	assertIntegerEq(t, r, 1)

	cDo(t, client1, "SET", "foo", "9998")
	cDo(t, client1, "INCR", "foo")
	r = cDo(t, client1, "OBJECT", "REFCOUNT", "foo")
	assertIntegerEq(t, r, 1)
	cDo(t, client1, "INCR", "foo")
	r = cDo(t, client1, "OBJECT", "REFCOUNT", "foo")
	assertIntegerEq(t, r, 1)
}

func Test_TCL_tcl_incr_fails_against_key_with_spaces_left(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "    11")
	r := cDo(t, client1, "INCR", "novar")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_incr_fails_against_key_with_spaces_right(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "11    ")
	r := cDo(t, client1, "INCR", "novar")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_incr_fails_against_key_with_spaces_both(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "    11    ")
	r := cDo(t, client1, "INCR", "novar")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_decrby_negation_overflow(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x", "0")
	r := cDo(t, client1, "DECRBY", "x", "-9223372036854775808")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_incr_fails_against_key_holding_a_list(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "RPUSH", "mylist", "1")
	r := cDo(t, client1, "INCR", "mylist")
	assertErrorPrefix(t, r, "WRONGTYPE")
	cDo(t, client1, "DEL", "mylist")
}

func Test_TCL_tcl_decrby_over_32bit_value_with_over_32bit_increment_negative_res(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "17179869184")
	r := cDo(t, client1, "DECRBY", "novar", "17179869185")
	assertIntegerEq(t, r, -1)
}

func Test_TCL_tcl_decrby_against_key_not_exist(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "key_not_exist")
	r := cDo(t, client1, "DECRBY", "key_not_exist", "1")
	assertIntegerEq(t, r, -1)
}

func Test_TCL_tcl_incrbyfloat_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "novar")
	r := cDo(t, client1, "INCRBYFLOAT", "novar", "1")
	assertBulkEq(t, r, "1")
	r = cDo(t, client1, "GET", "novar")
	assertBulkEq(t, r, "1")
	r = cDo(t, client1, "INCRBYFLOAT", "novar", "0.25")
	assertBulkEq(t, r, "1.25")
	r = cDo(t, client1, "GET", "novar")
	assertBulkEq(t, r, "1.25")
}

func Test_TCL_tcl_incrbyfloat_against_key_originally_set_with_set(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "1.5")
	r := cDo(t, client1, "INCRBYFLOAT", "novar", "1.5")
	assertBulkEq(t, r, "3")
}

func Test_TCL_tcl_incrbyfloat_over_32bit_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "17179869184")
	r := cDo(t, client1, "INCRBYFLOAT", "novar", "1.5")
	assertBulkEq(t, r, "17179869185.5")
}

func Test_TCL_tcl_incrbyfloat_over_32bit_value_with_over_32bit_increment(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "17179869184")
	r := cDo(t, client1, "INCRBYFLOAT", "novar", "17179869184")
	assertBulkEq(t, r, "34359738368")
}

func Test_TCL_tcl_incrbyfloat_fails_against_key_with_spaces_left(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "    11")
	r := cDo(t, client1, "INCRBYFLOAT", "novar", "1.0")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_incrbyfloat_fails_against_key_with_spaces_right(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "11    ")
	r := cDo(t, client1, "INCRBYFLOAT", "novar", "1.0")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_incrbyfloat_fails_against_key_with_spaces_both(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", " 11 ")
	r := cDo(t, client1, "INCRBYFLOAT", "novar", "1.0")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_incrbyfloat_fails_against_key_holding_a_list(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	cDo(t, client1, "RPUSH", "mylist", "1")
	r := cDo(t, client1, "INCRBYFLOAT", "mylist", "1.0")
	assertErrorPrefix(t, r, "WRONGTYPE")
	cDo(t, client1, "DEL", "mylist")
}

func Test_TCL_tcl_incrbyfloat_does_not_allow_nan_or_infinity(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "0")
	r := cDo(t, client1, "INCRBYFLOAT", "foo", "+inf")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_incrbyfloat_decrement(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "1")
	r := cDo(t, client1, "INCRBYFLOAT", "foo", "-1.1")
	assertFloatEq(t, r, -0.1)
}

func Test_TCL_tcl_string_to_double_with_null_terminator(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "1")
	cDo(t, client1, "SETRANGE", "foo", "2", "2")
	r := cDo(t, client1, "INCRBYFLOAT", "foo", "1")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_no_negative_zero(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	inc := "0.024390243902439025"
	dec := "-0.024390243902439025"
	cDo(t, client1, "INCRBYFLOAT", "foo", inc)
	cDo(t, client1, "INCRBYFLOAT", "foo", dec)
	r := cDo(t, client1, "GET", "foo")
	assertBulkEq(t, r, "0")
}

func Test_TCL_tcl_incrby_incrbyfloat_decrby_unhappy_path(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mykeyincr")

	r := cDo(t, client1, "INCR", "mykeyincr", "v")
	assertErrorPrefix(t, r, "ERR wrong number of arguments")
	r = cDo(t, client1, "DECR", "mykeyincr", "v")
	assertErrorPrefix(t, r, "ERR wrong number of arguments")

	r = cDo(t, client1, "INCRBY", "mykeyincr", "v")
	assertErrorPrefix(t, r, "ERR value is not an integer")
	r = cDo(t, client1, "INCRBY", "mykeyincr", "1.5")
	assertErrorPrefix(t, r, "ERR value is not an integer")
	r = cDo(t, client1, "DECRBY", "mykeyincr", "v")
	assertErrorPrefix(t, r, "ERR value is not an integer")
	r = cDo(t, client1, "DECRBY", "mykeyincr", "1.5")
	assertErrorPrefix(t, r, "ERR value is not an integer")
	r = cDo(t, client1, "INCRBYFLOAT", "mykeyincr", "v")
	assertErrorPrefix(t, r, "ERR value is not a valid float")
}
