package tcl

// Mechanical port of frogdb crates/redis-regression/tests/maxmemory_tcl.rs
// (Redis 8.6.0 unit scenarios).
import (
	"testing"
)
func Test_TCL_tcl_maxmemory_limit_honoured_allkeys_random(t *testing.T) {
	// PORT-TODO: test_limit_honoured("allkeys-random").await;
}

func Test_TCL_tcl_maxmemory_limit_honoured_allkeys_lru(t *testing.T) {
	// PORT-TODO: test_limit_honoured("allkeys-lru").await;
}

func Test_TCL_tcl_maxmemory_limit_honoured_allkeys_lfu(t *testing.T) {
	// PORT-TODO: test_limit_honoured("allkeys-lfu").await;
}

func Test_TCL_tcl_maxmemory_limit_honoured_volatile_lru(t *testing.T) {
	// PORT-TODO: test_limit_honoured("volatile-lru").await;
}

func Test_TCL_tcl_maxmemory_limit_honoured_volatile_lfu(t *testing.T) {
	// PORT-TODO: test_limit_honoured("volatile-lfu").await;
}

func Test_TCL_tcl_maxmemory_limit_honoured_volatile_random(t *testing.T) {
	// PORT-TODO: test_limit_honoured("volatile-random").await;
}

func Test_TCL_tcl_maxmemory_limit_honoured_volatile_ttl(t *testing.T) {
	// PORT-TODO: test_limit_honoured("volatile-ttl").await;
}

func Test_TCL_tcl_maxmemory_nonvolatile_allkeys_random(t *testing.T) {
	// PORT-TODO: test_nonvolatile_keys("allkeys-random").await;
}

func Test_TCL_tcl_maxmemory_nonvolatile_allkeys_lru(t *testing.T) {
	// PORT-TODO: test_nonvolatile_keys("allkeys-lru").await;
}

func Test_TCL_tcl_maxmemory_nonvolatile_volatile_lru(t *testing.T) {
	// PORT-TODO: test_nonvolatile_keys("volatile-lru").await;
}

func Test_TCL_tcl_maxmemory_nonvolatile_volatile_random(t *testing.T) {
	// PORT-TODO: test_nonvolatile_keys("volatile-random").await;
}

func Test_TCL_tcl_maxmemory_nonvolatile_volatile_ttl(t *testing.T) {
	// PORT-TODO: test_nonvolatile_keys("volatile-ttl").await;
}

func Test_TCL_tcl_maxmemory_volatile_only_volatile_lru(t *testing.T) {
	t.Skip("TODO: converter: frogdb test_volatile_only() helper has no Go counterpart")
}

func Test_TCL_tcl_maxmemory_volatile_only_volatile_lfu(t *testing.T) {
	t.Skip("TODO: converter: frogdb test_volatile_only() helper has no Go counterpart")
}

func Test_TCL_tcl_maxmemory_volatile_only_volatile_random(t *testing.T) {
	t.Skip("TODO: converter: frogdb test_volatile_only() helper has no Go counterpart")
}

func Test_TCL_tcl_maxmemory_volatile_only_volatile_ttl(t *testing.T) {
	t.Skip("TODO: converter: frogdb test_volatile_only() helper has no Go counterpart")
}

func Test_TCL_tcl_lru_lfu_value_of_key_just_added(t *testing.T) {
	t.Skip("TODO: only 13 of its statements could be ported")
}

func Test_TCL_tcl_maxmemory_eviction_due_to_output_buffers_of_mget_clients_client_eviction_false(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_maxmemory_eviction_due_to_output_buffers_of_mget_clients_client_eviction_true(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_maxmemory_eviction_due_to_input_buffer_of_dead_client_client_eviction_false(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_maxmemory_eviction_due_to_input_buffer_of_dead_client_client_eviction_true(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_maxmemory_eviction_due_to_output_buffers_of_pubsub_client_eviction_false(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_maxmemory_eviction_due_to_output_buffers_of_pubsub_client_eviction_true(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_maxmemory_client_tracking_no_eviction_feedback_loop(t *testing.T) {
	t.Skip("TODO: converter: cannot express Rust await expression")
}
