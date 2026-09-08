package goclient

// Crash recovery: run the real pebbisd binary, write data, SIGKILL it,
// restart on the same directory and verify nothing acknowledged was lost.
// This is the production-shaped durability gate.

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// buildBinary compiles cmd/pebbisd into a temp dir (once per test).
func buildBinary(t *testing.T) string {
	t.Helper()
	bin := t.TempDir() + "/pebbisd"
	cmd := exec.Command("go", "build", "-o", bin, "github.com/pebbis/pebbis/cmd/pebbisd")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build pebbisd: %v\n%s", err, out)
	}
	return bin
}

// freePort grabs an ephemeral port. There is a small race between Close and
// the child binding it, which is acceptable on a loopback test machine.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func startProcess(t *testing.T, bin, dir, addr string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin, "-addr", addr, "-dir", dir, "-appendfsync", "always")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start pebbisd: %v", err)
	}
	return cmd
}

func waitReady(t *testing.T, addr string) *redis.Client {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2})
		err := c.Ping(context.Background()).Err()
		if err == nil {
			return c
		}
		_ = c.Close()
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("server did not become ready")
	return nil
}

func TestKill9Recovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in short mode")
	}
	bin := buildBinary(t)
	dir := t.TempDir()
	addr := freePort(t)

	// Phase 1: write data against the live server.
	proc := startProcess(t, bin, dir, addr)
	c := waitReady(t, addr)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%d", i)
		if err := c.Set(ctx, key, fmt.Sprintf("value-%d", i), 0).Err(); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
	c.HSet(ctx, "hash", "f1", "v1", "f2", "v2")
	c.RPush(ctx, "list", "a", "b", "c")
	c.ZAdd(ctx, "z", redis.Z{Score: 1, Member: "one"}, redis.Z{Score: 2, Member: "two"})
	c.XGroupCreateMkStream(ctx, "q", "g", "0")
	msgID, err := c.XAdd(ctx, &redis.XAddArgs{Stream: "q", Values: map[string]interface{}{"j": 1}}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	if _, err := c.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "g", Consumer: "c1", Streams: []string{"q", ">"},
	}).Result(); err != nil {
		t.Fatalf("readgroup: %v", err)
	}
	// Expire is durable state too.
	c.Set(ctx, "shortlived", "v", 200*time.Millisecond)

	// Phase 2: SIGKILL - no graceful close, no checkpoint, WAL is all there is.
	if err := proc.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_, _ = proc.Process.Wait()
	_ = c.Close()
	// Give the expired key time to lapse across the restart.
	time.Sleep(300 * time.Millisecond)

	// Phase 3: restart on the same directory and verify.
	proc2 := startProcess(t, bin, dir, addr)
	defer func() { _ = proc2.Process.Kill() }()
	c2 := waitReady(t, addr)
	defer c2.Close()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%d", i)
		want := fmt.Sprintf("value-%d", i)
		if got, err := c2.Get(ctx, key).Result(); err != nil || got != want {
			t.Fatalf("after restart %s: got %q err %v", key, got, err)
		}
	}
	if got, err := c2.HGetAll(ctx, "hash").Result(); err != nil || len(got) != 2 || got["f1"] != "v1" {
		t.Fatalf("hash after restart: %v %v", got, err)
	}
	if got, err := c2.LRange(ctx, "list", 0, -1).Result(); err != nil || len(got) != 3 {
		t.Fatalf("list after restart: %v %v", got, err)
	}
	if got, err := c2.ZRangeWithScores(ctx, "z", 0, -1).Result(); err != nil || len(got) != 2 || got[0].Member != "one" {
		t.Fatalf("zset after restart: %+v %v", got, err)
	}
	// The expired key must not come back from the WAL.
	if err := c2.Get(ctx, "shortlived").Err(); err != redis.Nil {
		t.Fatalf("expired key survived restart: %v", err)
	}

	// Stream: the group and its PEL survive; the message is still pending.
	ext, err := c2.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: "q", Group: "g", Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil || len(ext) != 1 || ext[0].ID != msgID || ext[0].Consumer != "c1" {
		t.Fatalf("pending after restart: %+v %v", ext, err)
	}
	// And the write path works again after recovery.
	if err := c2.Set(ctx, "post-recovery", "ok", 0).Err(); err != nil {
		t.Fatalf("post-recovery write: %v", err)
	}
}
