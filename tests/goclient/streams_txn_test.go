package goclient

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestStreamConsumerGroup: the reliable-queue lifecycle through go-redis -
// group create (MKSTREAM), read, pending-list accounting, ack.
func TestStreamConsumerGroup(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	if err := c.XGroupCreateMkStream(ctx, "q", "workers", "0").Err(); err != nil {
		t.Fatalf("xgroup: %v", err)
	}
	ids := make([]string, 3)
	for i := range ids {
		res, err := c.XAdd(ctx, &redis.XAddArgs{
			Stream: "q",
			Values: map[string]interface{}{"job": i},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		ids[i] = res
	}

	read, err := c.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "workers",
		Consumer: "c1",
		Streams:  []string{"q", ">"},
		Count:    2,
	}).Result()
	if err != nil || len(read) != 1 || len(read[0].Messages) != 2 {
		t.Fatalf("xreadgroup: %v %+v", err, read)
	}

	pending, err := c.XPending(ctx, "q", "workers").Result()
	if err != nil || pending.Count != 2 {
		t.Fatalf("xpending: %+v %v", pending, err)
	}

	// Re-deliver via history mode: the retry counter must advance.
	hist, err := c.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "workers",
		Consumer: "c1",
		Streams:  []string{"q", "0"},
	}).Result()
	if err != nil || len(hist) == 0 || len(hist[0].Messages) != 2 {
		t.Fatalf("history read: %v %+v", err, hist)
	}
	ext, err := c.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: "q", Group: "workers", Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("xpendingext: %v", err)
	}
	for _, row := range ext {
		if row.RetryCount != 2 {
			t.Errorf("retry count: want 2, got %d for %s", row.RetryCount, row.ID)
		}
	}

	// Ack drains the PEL for the delivered IDs.
	if n, err := c.XAck(ctx, "q", "workers", ids[0], ids[1]).Result(); err != nil || n != 2 {
		t.Fatalf("xack: %d %v", n, err)
	}
	// ids[2] was never delivered (COUNT 2), so the PEL is now empty.
	if pending, err = c.XPending(ctx, "q", "workers").Result(); err != nil || pending.Count != 0 {
		t.Fatalf("xpending after ack: %+v %v", pending, err)
	}
}

// TestTransactions: MULTI/EXEC through TxPipeline, and optimistic locking
// through Watch - the watch-dirty case must fail the transaction.
func TestTransactions(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	// TxPipeline wraps the commands in MULTI/EXEC.
	if err := c.TxPipeline().Set(ctx, "txk", "v1", 0).Err(); err != nil {
		t.Fatalf("txpipeline set: %v", err)
	}

	// Watch + modify from another connection: EXEC must report failure.
	other := redis.NewClient(&redis.Options{Addr: c.Options().Addr, Protocol: 2})
	defer other.Close()

	if err := c.Set(ctx, "w", "original", 0).Err(); err != nil {
		t.Fatal(err)
	}
	err := c.Watch(ctx, func(tx *redis.Tx) error {
		// Another writer sneaks in between WATCH and EXEC.
		if err := other.Set(ctx, "w", "sneaky", 0).Err(); err != nil {
			return err
		}
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "w", "from-txn", 0)
			return nil
		})
		return err
	}, "w")
	if err == nil {
		t.Fatal("watched transaction succeeded despite concurrent modification")
	}
	if got, err := c.Get(ctx, "w").Result(); err != nil || got != "sneaky" {
		t.Fatalf("after failed txn: %q %v", got, err)
	}

	// Without interference the watched transaction commits.
	err = c.Watch(ctx, func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "w", "from-txn", 0)
			return nil
		})
		return err
	}, "w")
	if err != nil {
		t.Fatalf("clean watched txn: %v", err)
	}
}

// TestPipelineOrdering: a batched pipeline must reply in request order even
// when mixing reads and writes.
func TestPipelineOrdering(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	pipe := c.Pipeline()
	set := pipe.Set(ctx, "p1", "v1", 0)
	get := pipe.Get(ctx, "p1")
	cnt := pipe.DBSize(ctx)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("exec: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Errorf("set: %v", err)
	}
	if v, err := get.Result(); err != nil || v != "v1" {
		t.Errorf("get: %q %v", v, err)
	}
	if n, err := cnt.Result(); err != nil || n < 1 {
		t.Errorf("dbsize: %d %v", n, err)
	}
}

// TestPubSubDelivery: subscribe on one client, publish on another, receive a
// typed message; then unsubscribe cleanly.
func TestPubSubDelivery(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	sub := c.Subscribe(ctx, "news")
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe confirm: %v", err)
	}

	// Give the subscription a moment to register server-side.
	time.Sleep(50 * time.Millisecond)
	if n, err := c.Publish(ctx, "news", "hello").Result(); err != nil || n != 1 {
		t.Fatalf("publish: %d %v", n, err)
	}
	select {
	case msg := <-sub.Channel():
		if msg.Channel != "news" || msg.Payload != "hello" {
			t.Fatalf("message: %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message within 2s")
	}
}

// TestBlockingPop: BLPOP blocks and is served by a later push, matching
// Redis' nil-on-timeout contract.
func TestBlockingPop(t *testing.T) {
	c, _ := newClientOn(t)
	c2, _ := newClientOn(t)
	// Both clients target distinct servers; use one server explicitly.
	addr := c.Options().Addr
	c2.Close()
	c2 = redis.NewClient(&redis.Options{Addr: addr, Protocol: 2})
	defer c2.Close()
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(100 * time.Millisecond)
		if err := c2.RPush(ctx, "bq", "work").Err(); err != nil {
			t.Errorf("rpush: %v", err)
		}
	}()
	vals, err := c.BLPop(ctx, 2*time.Second, "bq").Result()
	<-done
	if err != nil || len(vals) != 2 || vals[0] != "bq" || vals[1] != "work" {
		t.Fatalf("blpop: %v %v", vals, err)
	}

	// Timed-out BLPOP replies nil, surfaced as redis.Nil.
	if err := c.Set(ctx, "other", "x", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.BLPop(ctx, 100*time.Millisecond, "empty").Result(); err != redis.Nil {
		t.Fatalf("blpop timeout: want redis.Nil, got %v", err)
	}
}
