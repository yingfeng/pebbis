package tcl

// Mechanical port of frogdb crates/redis-regression/tests/stream_tcl.rs
// (Redis 8.6.0 unit/stream.tcl scenarios).

import (
	"fmt"
	"testing"
)

func Test_TCL_tcl_xadd_wrong_number_of_args(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "XADD", "mystream")
	_ = resp
	assertErrorPrefix(t, resp, "ERR")
	resp = cDo(t, client1, "XADD", "mystream", "*")
	assertErrorPrefix(t, resp, "ERR")
	resp = cDo(t, client1, "XADD", "mystream", "*", "field")
	assertErrorPrefix(t, resp, "ERR")
}







func Test_TCL_tcl_xadd_id_overflow_error(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "XADD", "mystream", "18446744073709551615-18446744073709551615", "k", "v")
	resp := cDo(t, client1, "XADD", "mystream", "*", "k", "v")
	_ = resp
	assertErrorPrefix(t, resp, "ERR")
}





func Test_TCL_tcl_xadd_auto_seq_cant_be_smaller_than_last_id(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "XADD", "mystream", "123-456", "k", "v")
	resp := cDo(t, client1, "XADD", "mystream", "42-*", "k", "v")
	_ = resp
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_xadd_auto_seq_cant_overflow(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "XADD", "mystream", "1-18446744073709551615", "k", "v")
	resp := cDo(t, client1, "XADD", "mystream", "1-*", "k", "v")
	_ = resp
	assertErrorPrefix(t, resp, "ERR")
}



















func Test_TCL_tcl_xrange_count_works(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	for i := 0; i <= 100; i++ {
		cDo(t, client1, "XADD", "mystream", "*", "item", fmt.Sprint(i))
	}
	resp := cDo(t, client1, "XRANGE", "mystream", "-", "+", "COUNT", "10")
	_ = resp
	entries := unwrapArray(t, resp)
	_ = entries
	assertArrayLen(t, entries, 10)
}

func Test_TCL_tcl_xrevrange_count_works(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	for i := 0; i <= 100; i++ {
		cDo(t, client1, "XADD", "mystream", "*", "item", fmt.Sprint(i))
	}
	resp := cDo(t, client1, "XREVRANGE", "mystream", "+", "-", "COUNT", "10")
	_ = resp
	entries := unwrapArray(t, resp)
	_ = entries
	assertArrayLen(t, entries, 10)
}





func Test_TCL_tcl_xread_non_blocking_empty_stream(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "XREAD", "COUNT", "10", "STREAMS", "nonexistent", "0-0")
	_ = resp
	assertNil(t, resp)
}





func Test_TCL_tcl_xread_last_element_from_non_empty_stream(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "XADD", "s1", "1-1", "a", "1")
	cDo(t, client1, "XADD", "s1", "1-2", "b", "2")
	cDo(t, client1, "XADD", "s1", "1-3", "c", "3")
	resp := cDo(t, client1, "XREAD", "STREAMS", "s1", "$")
	_ = resp
	assertNil(t, resp)
}

func Test_TCL_tcl_xread_last_element_from_empty_stream(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "XREAD", "STREAMS", "s1", "$")
	_ = resp
	assertNil(t, resp)
}





































































