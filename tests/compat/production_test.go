package compat

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redistore/redistore"
	"github.com/redistore/redistore/config"
	"github.com/tidwall/resp"
)

// Production-facing behaviour: authentication, transactions, pub/sub,
// observability and eviction policies.

func TestAuthRequired(t *testing.T) {
	c := setupWith(t, func(cfg *config.Config) { cfg.RequirePass = "s3cret" })

	// Every command but AUTH is refused before authenticating.
	assertErr(t, do(t, c, "PING"), "NOAUTH")
	assertErr(t, do(t, c, "GET", "k"), "NOAUTH")

	assertErr(t, do(t, c, "AUTH", "wrong"), "WRONGPASS")
	assertStr(t, do(t, c, "AUTH", "s3cret"), "OK")
	assertStr(t, do(t, c, "PING"), "PONG")
	preset(t, c, "k", "v")
	assertStr(t, do(t, c, "GET", "k"), "v")
}

func TestAuthNotConfigured(t *testing.T) {
	c := setup(t)
	// With no password set, AUTH is an error rather than silently succeeding.
	assertErr(t, do(t, c, "AUTH", "x"), "no password is set")
}

func TestMultiExec(t *testing.T) {
	c := setup(t)

	assertStr(t, do(t, c, "MULTI"), "OK")
	assertStr(t, do(t, c, "SET", "a", "1"), "QUEUED")
	assertStr(t, do(t, c, "INCR", "n"), "QUEUED")
	assertStr(t, do(t, c, "GET", "a"), "QUEUED")

	res := do(t, c, "EXEC").Array()
	if len(res) != 3 {
		t.Fatalf("EXEC returned %d replies, want 3", len(res))
	}
	assertStr(t, res[0], "OK")
	assertInt(t, res[1], 1)
	assertStr(t, res[2], "1")

	// The effects are visible afterwards.
	assertStr(t, do(t, c, "GET", "a"), "1")
	assertStr(t, do(t, c, "GET", "n"), "1")
}

func TestMultiDiscard(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "MULTI"), "OK")
	assertStr(t, do(t, c, "SET", "a", "1"), "QUEUED")
	assertStr(t, do(t, c, "DISCARD"), "OK")
	assertInt(t, do(t, c, "EXISTS", "a"), 0)
	assertErr(t, do(t, c, "EXEC"), "EXEC without MULTI")
}

func TestMultiNestedIsRejected(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "MULTI"), "OK")
	assertErr(t, do(t, c, "MULTI"), "MULTI calls can not be nested")
	assertStr(t, do(t, c, "DISCARD"), "OK")
}

func TestWatchAbortsOnChange(t *testing.T) {
	c := setup(t)
	preset(t, c, "w", "orig")

	assertStr(t, do(t, c, "WATCH", "w"), "OK")
	assertStr(t, do(t, c, "MULTI"), "OK")
	assertStr(t, do(t, c, "SET", "w", "changed"), "QUEUED")
	// EXEC answers with an array of the queued replies.
	execd := do(t, c, "EXEC").Array()
	if len(execd) != 1 {
		t.Fatalf("EXEC returned %d replies, want 1", len(execd))
	}
	assertStr(t, execd[0], "OK")
	assertStr(t, do(t, c, "GET", "w"), "changed")

	// Watch again, then write to the key *outside* the transaction: the version
	// moves, so EXEC must abort.
	assertStr(t, do(t, c, "WATCH", "w"), "OK")
	assertStr(t, do(t, c, "SET", "w", "direct"), "OK")
	assertStr(t, do(t, c, "MULTI"), "OK")
	assertStr(t, do(t, c, "SET", "w", "txn"), "QUEUED")
	if v := do(t, c, "EXEC"); !v.IsNull() {
		t.Errorf("EXEC after a watched key changed should be nil, got %q", v.String())
	}
	assertStr(t, do(t, c, "GET", "w"), "direct")
}

func TestUnwatch(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "WATCH", "a", "b"), "OK")
	assertStr(t, do(t, c, "UNWATCH"), "OK")
	assertStr(t, do(t, c, "MULTI"), "OK")
	assertStr(t, do(t, c, "SET", "a", "1"), "QUEUED")
	assertStr(t, do(t, c, "EXEC").Array()[0], "OK")
}

func TestPublishSubscribe(t *testing.T) {
	c := setup(t)

	// With no subscribers, PUBLISH reports zero receivers.
	assertInt(t, do(t, c, "PUBLISH", "ch", "hello"), 0)

	// A real subscriber needs its own connection; SUBSCRIBE detaches it.
	sub := subscribeConn(t, c, "ch")
	defer func() { _ = sub.Close() }()

	// Give the subscription a moment to register.
	deadline := time.Now().Add(2 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		n = do(t, c, "PUBLISH", "ch", "hello").Integer()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n != 1 {
		t.Fatalf("PUBLISH reached %d subscribers, want 1", n)
	}

	// The subscriber should have received the message.
	_ = sub.SetDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 512)
	rn, err := sub.Read(buf)
	if err != nil {
		t.Fatalf("subscriber read: %v", err)
	}
	got := string(buf[:rn])
	if !contains(got, "message") || !contains(got, "hello") {
		t.Errorf("subscriber received %q, want a message containing 'hello'", got)
	}
}

func TestPubSubChannels(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "PUBLISH", "news", "x"), 0)
	// Without a subscriber connection the channel list is empty, which is the
	// honest answer for this server instance.
	if n := len(do(t, c, "PUBSUB", "CHANNELS").Array()); n != 0 {
		t.Errorf("PUBSUB CHANNELS = %d, want 0 with no subscribers", n)
	}
	assertInt(t, do(t, c, "PUBSUB", "NUMPAT"), 0)
}

// subscribeConn opens a second connection to the same server as c and issues
// SUBSCRIBE. It returns the raw socket so the test can read pushed messages.
func subscribeConn(t *testing.T, c *resp.Conn, channels ...string) net.Conn {
	t.Helper()
	addr := serverAddrOf(t, c)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial for subscribe: %v", err)
	}
	sc := resp.NewConn(conn)
	args := append([]string{"SUBSCRIBE"}, channels...)
	vals := make([]resp.Value, len(args))
	for i, a := range args {
		vals[i] = resp.StringValue(a)
	}
	if err := sc.WriteArray(vals); err != nil {
		_ = conn.Close()
		t.Fatalf("subscribe write: %v", err)
	}
	return conn
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func TestSlowLog(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "SLOWLOG", "RESET"), "OK")
	assertInt(t, do(t, c, "SLOWLOG", "LEN"), 0)

	// Lower the threshold so ordinary commands get recorded.
	assertStr(t, do(t, c, "CONFIG", "SET", "slowlog-log-slower-than", "0"), "OK")
	preset(t, c, "k", "v")
	_ = do(t, c, "GET", "k")

	entries := do(t, c, "SLOWLOG", "GET").Array()
	if len(entries) == 0 {
		t.Fatal("SLOWLOG GET returned nothing after lowering the threshold")
	}
	// Each entry is [id, timestamp, duration, [args...]].
	first := entries[0].Array()
	if len(first) != 4 {
		t.Fatalf("slowlog entry has %d fields, want 4", len(first))
	}
	if first[2].Integer() < 0 {
		t.Errorf("slowlog duration = %d", first[2].Integer())
	}

	assertStr(t, do(t, c, "CONFIG", "SET", "slowlog-log-slower-than", "10000"), "OK")
	assertStr(t, do(t, c, "SLOWLOG", "RESET"), "OK")
	assertInt(t, do(t, c, "SLOWLOG", "LEN"), 0)
}

func TestClientCommands(t *testing.T) {
	c := setup(t)
	if do(t, c, "CLIENT", "ID").Integer() <= 0 {
		t.Error("CLIENT ID should be positive")
	}
	assertStr(t, do(t, c, "CLIENT", "SETNAME", "tester"), "OK")
	assertStr(t, do(t, c, "CLIENT", "GETNAME"), "tester")
	info := do(t, c, "CLIENT", "INFO").String()
	if !contains(info, "name=tester") {
		t.Errorf("CLIENT INFO = %q, want it to mention name=tester", info)
	}
	if !contains(do(t, c, "CLIENT", "LIST").String(), "name=tester") {
		t.Error("CLIENT LIST should include the connection")
	}
}

func TestConfigSetRuntime(t *testing.T) {
	c := setup(t)

	assertStr(t, do(t, c, "CONFIG", "SET", "maxmemory", "16mb"), "OK")
	res := do(t, c, "CONFIG", "GET", "maxmemory").Array()
	if res[1].Integer() != 16<<20 {
		t.Errorf("maxmemory = %d, want %d", res[1].Integer(), 16<<20)
	}

	assertStr(t, do(t, c, "CONFIG", "SET", "maxmemory-samples", "10"), "OK")
	if v := do(t, c, "CONFIG", "GET", "maxmemory-samples").Array()[1].Integer(); v != 10 {
		t.Errorf("maxmemory-samples = %d, want 10", v)
	}

	assertErr(t, do(t, c, "CONFIG", "SET", "databases", "32"), "not supported")
	assertErr(t, do(t, c, "CONFIG", "SET", "maxmemory", "not-a-number"), "Invalid")
}

// TestConfigSetConcurrentWithCommands hammers CONFIG SET (slowlog + eviction
// knobs) while other goroutines run ordinary commands. The slowlog thresholds
// and the eviction knobs are read on the hot path of every command without a
// mutex, so a race here means the atomic-config plumbing is broken. Under
// -race this fails loudly if the read/write sites are not synchronised.
//
// Each goroutine uses its own connection: the RESP client is not safe for
// concurrent use, but the server's config access is, and that is what we test.
func TestConfigSetConcurrentWithCommands(t *testing.T) {
	c := setup(t)

	const writers = 8
	const rounds = 2000
	var wg sync.WaitGroup

	// Mutators: flip slowlog + eviction knobs back and forth.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mc, mraw, err := dialAnother(t, c)
			if err != nil {
				t.Errorf("dial mutator: %v", err)
				return
			}
			defer mraw.Close()
			for r := 0; r < rounds; r++ {
				_ = do(t, mc, "CONFIG", "SET", "slowlog-log-slower-than", strconv.Itoa(r%2*1000-1))
				_ = do(t, mc, "CONFIG", "SET", "slowlog-max-len", strconv.Itoa((r%8)+1))
				_ = do(t, mc, "CONFIG", "SET", "maxmemory-samples", strconv.Itoa((r%9)+1))
			}
		}()
	}

	// Workers: ordinary traffic that touches the slowlog record() hot path and
	// the eviction snapshot.
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			wc, wraw, err := dialAnother(t, c)
			if err != nil {
				t.Errorf("dial worker: %v", err)
				return
			}
			defer wraw.Close()
			for r := 0; r < rounds; r++ {
				_ = do(t, wc, "SET", "k"+strconv.Itoa(id), "v")
				_ = do(t, wc, "GET", "k"+strconv.Itoa(id))
				_ = do(t, wc, "SLOWLOG", "LEN")
			}
		}(i)
	}

	wg.Wait()
}

// dialAnother opens a fresh connection to the same server as c, using the
// address book maintained by setupWith. It returns the RESP client and the
// underlying net.Conn (which the caller must Close, since resp.Conn has no
// Close of its own).
func dialAnother(t *testing.T, c *resp.Conn) (*resp.Conn, net.Conn, error) {
	t.Helper()
	addr, ok := addrs.Load(c)
	if !ok {
		return nil, nil, fmt.Errorf("no address recorded for client")
	}
	conn, err := net.Dial("tcp", addr.(string))
	if err != nil {
		return nil, nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	return resp.NewConn(conn), conn, nil
}

// TestScanCollectionPagination verifies that HSCAN/SSCAN/ZSCAN honour COUNT and
// return a real, resumable cursor (previously they ignored COUNT and always
// returned the whole collection with cursor 0). A full iteration following the
// cursor must still yield every element exactly once.
//
// stride is the number of array elements per logical entry: 2 for HSCAN/ZSCAN
// (field,value / member,score), 1 for SSCAN (member).
func TestScanCollectionPagination(t *testing.T) {
	c := setup(t)

	for i := 0; i < 25; i++ {
		_ = do(t, c, "HSET", "h", fmt.Sprintf("f%02d", i), "v")
		_ = do(t, c, "SADD", "s", fmt.Sprintf("m%02d", i))
		_ = do(t, c, "ZADD", "z", strconv.Itoa(i), fmt.Sprintf("m%02d", i))
	}

	// collect walks the cursor to 0 and returns the set of logical entries.
	// The cursor is the argument right after the key, before any MATCH/COUNT.
	collect := func(stride int, cmd ...string) map[string]int {
		seen := map[string]int{}
		cursor := "0"
		for {
			args := append([]string{cmd[0], cmd[1], cursor}, cmd[2:]...)
			v := do(t, c, args...)
			if v.Type() == resp.Error {
				t.Fatalf("command %v returned error: %s", args, v.String())
			}
			res := v.Array()
			cursor = res[0].String()
			elems := res[1].Array()
			for i := 0; i+stride <= len(elems); i += stride {
				seen[elems[i].String()]++
			}
			if cursor == "0" {
				break
			}
		}
		return seen
	}

	if got := len(collect(2, "HSCAN", "h")); got != 25 {
		t.Errorf("HSCAN collected %d fields, want 25", got)
	}
	if got := len(collect(1, "SSCAN", "s")); got != 25 {
		t.Errorf("SSCAN collected %d members, want 25", got)
	}
	if got := len(collect(2, "ZSCAN", "z")); got != 25 {
		t.Errorf("ZSCAN collected %d members, want 25", got)
	}

	// COUNT 5 on 25 fields must paginate: first page returns ≤5 fields (≤10 array
	// elements) and a non-zero cursor.
	v := do(t, c, "HSCAN", "h", "0", "COUNT", "5")
	if v.Type() == resp.Error {
		t.Fatalf("HSCAN COUNT 5 returned error: %s", v.String())
	}
	res := v.Array()
	if got := len(res[1].Array()); got != 10 {
		t.Errorf("HSCAN COUNT 5 first page returned %d array elems, want 10", got)
	}
	if res[0].String() == "0" {
		t.Error("HSCAN COUNT 5 should not finish in one page with 25 fields")
	}

	// COUNT with MATCH: f1* matches f10..f19 (10 fields).
	if got := len(collect(2, "HSCAN", "h", "MATCH", "f1*")); got != 10 {
		t.Errorf("HSCAN MATCH f1* collected %d, want 10", got)
	}
}

func TestObjectIntrospection(t *testing.T) {
	c := setup(t)
	preset(t, c, "s", "hello")
	assertStr(t, do(t, c, "OBJECT", "ENCODING", "s"), "embstr")
	if v := do(t, c, "OBJECT", "FREQ", "s"); !v.IsNull() && v.Type() != resp.Error {
		t.Errorf("OBJECT FREQ without an LFU policy should error or be nil, got %q", v.String())
	}
	assertInt(t, do(t, c, "OBJECT", "REFCOUNT", "s"), 1)
	if v := do(t, c, "OBJECT", "ENCODING", "missing"); !v.IsNull() {
		t.Errorf("OBJECT ENCODING on a missing key should be nil, got %q", v.String())
	}
}

func TestLFUEviction(t *testing.T) {
	c := setupWith(t, func(cfg *config.Config) {
		cfg.MaxMemory = 4096
		cfg.EvictionPolicy = config.AllKeysLFU
		cfg.EvictionMode = config.EvictCache
	})

	for i := range 1500 {
		_ = do(t, c, "SET", "key:"+itoa(i), "value-"+itoa(i))
	}
	// With LFU and cache mode, some keys must have been dropped for good.
	var missing int
	for i := range 1500 {
		if v := do(t, c, "GET", "key:"+itoa(i)); v.IsNull() {
			missing++
		}
	}
	if missing == 0 {
		t.Fatal("LFU eviction in cache mode should have dropped keys")
	}
	t.Logf("LFU dropped %d/1500 keys", missing)
}

// TestEvictionPersistKeepsDataServed is the property that makes the whole
// design worth it: memory is bounded, yet nothing is lost.
func TestEvictionPersistKeepsDataServed(t *testing.T) {
	c := setupWith(t, func(cfg *config.Config) {
		cfg.MaxMemory = 4096
		cfg.EvictionPolicy = config.AllKeysLRU
		cfg.EvictionMode = config.EvictPersist
	})

	const n = 800
	for i := range n {
		_ = do(t, c, "SET", "key:"+itoa(i), "value-"+itoa(i))
	}
	for i := range n {
		want := "value-" + itoa(i)
		if got := do(t, c, "GET", "key:"+itoa(i)); got.String() != want {
			t.Fatalf("key %d: got %q, want %q", i, got.String(), want)
		}
	}
}

var _ = redistore.Version
