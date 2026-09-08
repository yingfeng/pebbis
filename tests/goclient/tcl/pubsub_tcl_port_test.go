package tcl

// Mechanical port of frogdb crates/redis-regression/tests/pubsub_tcl.rs
// (Redis 8.6.0 unit scenarios).
import (
	"testing"
)
func Test_TCL_tcl_pubsub_ping_resp2(t *testing.T) {
	t.Skip("TODO: only 18 of its statements could be ported")
}

func Test_TCL_tcl_publish_subscribe_basics(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_publish_subscribe_with_two_clients(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_publish_subscribe_after_unsubscribe_without_arguments(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_subscribe_to_one_channel_more_than_once(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_unsubscribe_from_non_subscribed_channels(t *testing.T) {
	t.Skip("TODO: only 14 of its statements could be ported")
}

func Test_TCL_tcl_publish_psubscribe_basics(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_publish_psubscribe_with_two_clients(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_publish_psubscribe_after_punsubscribe_without_arguments(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_punsubscribe_from_non_subscribed_channels(t *testing.T) {
	t.Skip("TODO: only 14 of its statements could be ported")
}

func Test_TCL_tcl_numsub_returns_numbers_not_strings(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	resp := cDoConn(t, client1, "PUBSUB", "NUMSUB", "abc", "def")
	items := unwrapArray(t, resp)
	if len(items) != 4 { t.Fatalf("len mismatch: got %d want 4", len(items)) }
	assertBulkEq(t, items[0], "abc")
	// PORT-TODO: assert_eq!(unwrap_integer(&items[1]), 0);
	assertBulkEq(t, items[2], "def")
	// PORT-TODO: assert_eq!(unwrap_integer(&items[3]), 0);
}

func Test_TCL_tcl_numpat_returns_number_of_unique_patterns(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_mix_subscribe_and_psubscribe(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_punsubscribe_and_unsubscribe_should_always_reply(t *testing.T) {
	addr := startServer(t)
	client1 := connectConn(t, addr)
	resp := cDoConn(t, client1, "PUNSUBSCRIBE")
	items := unwrapArray(t, resp)
	assertBulkEq(t, items[0], "punsubscribe")
	// PORT-TODO: assert_eq!(unwrap_integer(&items[2]), 0);
	resp2 := cDoConn(t, client1, "UNSUBSCRIBE")
	items2 := unwrapArray(t, resp2)
	assertBulkEq(t, items2[0], "unsubscribe")
	// PORT-TODO: assert_eq!(unwrap_integer(&items2[2]), 0);
}

func Test_TCL_tcl_pubsub_channels_lists_active_channels(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_pubsub_numsub_with_active_subscriptions(t *testing.T) {
	t.Skip("TODO: needs multiple client connections")
}

func Test_TCL_tcl_pubsub_ping_resp3(t *testing.T) {
	t.Skip("TODO: its client connection is opened by a frogdb helper")
}

func Test_TCL_tcl_pubsub_messages_with_client_reply_off(t *testing.T) {
	t.Skip("TODO: only 9 of its statements could be ported")
}

func Test_TCL_tcl_publish_to_self_inside_multi(t *testing.T) {
	t.Skip("TODO: its client connection is opened by a frogdb helper")
}

func Test_TCL_tcl_unsubscribe_inside_multi_and_publish_to_self(t *testing.T) {
	t.Skip("TODO: its client connection is opened by a frogdb helper")
}
