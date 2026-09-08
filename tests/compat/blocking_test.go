package compat

import (
	"net"
	"testing"
	"time"

	"github.com/tidwall/resp"
)

// Blocking list commands.
//
// These need two connections: one parked in BLPOP, another to push the element
// that unblocks it.

// secondConn opens another connection to the same server as c.
func secondConn(t *testing.T, c *resp.Conn) *resp.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", serverAddrOf(t, c))
	if err != nil {
		t.Fatalf("dial second connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	return resp.NewConn(conn)
}

func TestBLPopImmediate(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "RPUSH", "l", "a", "b"), 2)

	// A non-empty list is served without blocking.
	res := do(t, c, "BLPOP", "l", "1").Array()
	if len(res) != 2 {
		t.Fatalf("BLPOP returned %d elements, want 2", len(res))
	}
	assertStr(t, res[0], "l")
	assertStr(t, res[1], "a")
	assertInt(t, do(t, c, "LLEN", "l"), 1)
}

func TestBLPopTimeout(t *testing.T) {
	c := setup(t)
	// A timeout with no data waits out the deadline and replies nil.
	// (Timeout 0 means block forever, so it is deliberately not exercised here
	// from the main test goroutine.)
	start := time.Now()
	res := do(t, c, "BLPOP", "empty", "1")
	if !res.IsNull() {
		t.Fatalf("BLPOP that times out should reply nil, got %q", res.String())
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("BLPOP returned after %v, want at least ~1s", elapsed)
	}
}

// TestBLPopWokenByPush is the interesting one: the command must be released by
// a push on another connection, not by its own timeout.
func TestBLPopWokenByPush(t *testing.T) {
	c := setup(t)
	pusher := secondConn(t, c)

	type result struct {
		res resp.Value
		err error
	}
	done := make(chan result, 1)
	go func() {
		// Block for up to 5s; the push should release it far sooner.
		v, _, err := sendAndRead(t, c, "BLPOP", "jobs", "5")
		done <- result{v, err}
	}()

	time.Sleep(150 * time.Millisecond)
	assertInt(t, do(t, pusher, "RPUSH", "jobs", "work"), 1)

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("BLPOP: %v", r.err)
		}
		arr := r.res.Array()
		if len(arr) != 2 {
			t.Fatalf("BLPOP returned %d elements, want 2", len(arr))
		}
		assertStr(t, arr[0], "jobs")
		assertStr(t, arr[1], "work")
	case <-time.After(4 * time.Second):
		t.Fatal("BLPOP was not woken by the push")
	}
}

func TestBRPopWokenByPush(t *testing.T) {
	c := setup(t)
	pusher := secondConn(t, c)
	assertInt(t, do(t, pusher, "RPUSH", "l", "x"), 1)

	// BRPOP takes from the tail.
	res := do(t, c, "BRPOP", "l", "1").Array()
	if len(res) != 2 || res[1].String() != "x" {
		t.Fatalf("BRPOP = %v, want [l x]", res)
	}
}

func TestBLPopMultipleKeys(t *testing.T) {
	c := setup(t)
	pusher := secondConn(t, c)

	done := make(chan resp.Value, 1)
	go func() {
		v, _, err := sendAndRead(t, c, "BLPOP", "a", "b", "c", "5")
		if err != nil {
			t.Errorf("BLPOP: %v", err)
		}
		done <- v
	}()

	time.Sleep(150 * time.Millisecond)
	// Only the second key ever receives data; the reply must name it.
	assertInt(t, do(t, pusher, "RPUSH", "b", "found"), 1)

	select {
	case v := <-done:
		arr := v.Array()
		if len(arr) != 2 || arr[0].String() != "b" || arr[1].String() != "found" {
			t.Fatalf("BLPOP = %v, want [b found]", v)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("BLPOP was not woken")
	}
}

func TestBLPopValidation(t *testing.T) {
	c := setup(t)
	assertErr(t, do(t, c, "BLPOP", "l"), errWrongArgs)
	assertErr(t, do(t, c, "BLPOP", "l", "-1"), "timeout is negative")
	assertErr(t, do(t, c, "BLPOP", "l", "abc"), "timeout is not a float or out of range")
}

func TestRPopLPush(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "RPUSH", "src", "a", "b", "c"), 3)

	assertStr(t, do(t, c, "RPOPLPUSH", "src", "dst"), "c")
	assertStr(t, do(t, c, "RPOPLPUSH", "src", "dst"), "b")
	assertInt(t, do(t, c, "LLEN", "src"), 1)
	// dst receives at the head, so the order is reversed.
	assertStr(t, do(t, c, "LINDEX", "dst", "0"), "b")
	assertStr(t, do(t, c, "LINDEX", "dst", "1"), "c")

	if v := do(t, c, "RPOPLPUSH", "nosuch", "dst"); !v.IsNull() {
		t.Errorf("RPOPLPUSH on a missing source should be nil, got %q", v.String())
	}
}

// sendAndRead writes one command and reads one reply, for use from a goroutine.
func sendAndRead(t *testing.T, c *resp.Conn, args ...string) (resp.Value, int, error) {
	t.Helper()
	vals := make([]resp.Value, len(args))
	for i, a := range args {
		vals[i] = resp.StringValue(a)
	}
	if err := c.WriteArray(vals); err != nil {
		return resp.Value{}, 0, err
	}
	return c.ReadValue()
}
