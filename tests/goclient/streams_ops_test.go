package goclient

// Stream variants and blocking commands beyond the consumer-group lifecycle.

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestStreamVariants(t *testing.T) {
	c, _ := newClientOn(t)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		if _, err := c.XAdd(ctx, &redis.XAddArgs{
			Stream: "s", ID: mustID(i), Values: map[string]interface{}{"i": i},
		}).Result(); err != nil {
			t.Fatalf("xadd %d: %v", i, err)
		}
	}
	if n, err := c.XLen(ctx, "s").Result(); err != nil || n != 5 {
		t.Fatalf("xlen: %d %v", n, err)
	}

	// Plain (non-group) read from the beginning.
	read, err := c.XRead(ctx, &redis.XReadArgs{Streams: []string{"s", "0"}, Count: 2}).Result()
	if err != nil || len(read) != 1 || len(read[0].Messages) != 2 {
		t.Fatalf("xread: %v %+v", err, read)
	}

	// XRANGE / XREVRANGE.
	rng, err := c.XRange(ctx, "s", "1-1", "3-1").Result()
	if err != nil || len(rng) != 3 || rng[0].ID != "1-1" || rng[2].ID != "3-1" {
		t.Fatalf("xrange: %+v %v", rng, err)
	}
	rev, err := c.XRevRange(ctx, "s", "+", "-").Result()
	if err != nil || len(rev) != 5 || rev[0].ID != "5-1" {
		t.Fatalf("xrevrange: %+v %v", rev, err)
	}

	// XDEL removes an entry; XLEN drops.
	if n, err := c.XDel(ctx, "s", "3-1").Result(); err != nil || n != 1 {
		t.Fatalf("xdel: %d %v", n, err)
	}
	if n, err := c.XLen(ctx, "s").Result(); err != nil || n != 4 {
		t.Fatalf("xlen after del: %d %v", n, err)
	}

	// XTRIM by maxlen.
	if n, err := c.XTrimMaxLen(ctx, "s", 2).Result(); err != nil || n != 2 {
		t.Fatalf("xtrim: %d %v", n, err)
	}

	// XCLAIM: build a group + PEL, then claim by another consumer.
	c.XGroupCreateMkStream(ctx, "s", "g", "0")
	for i := 0; i < 2; i++ {
		c.XAdd(ctx, &redis.XAddArgs{Stream: "s", Values: map[string]interface{}{"j": i}})
	}
	read, err = c.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "g", Consumer: "c1", Streams: []string{"s", ">"},
	}).Result()
	// The group was created with last-delivered 0, so the two trimmed
	// survivors are still undelivered: 2 + 2 fresh = 4 messages.
	if err != nil || len(read) != 1 || len(read[0].Messages) != 4 {
		t.Fatalf("readgroup: %v %+v", err, read)
	}
	msgIDs := []string{read[0].Messages[0].ID, read[0].Messages[1].ID}
	claimed, err := c.XClaim(ctx, &redis.XClaimArgs{
		Stream:   "s",
		Group:    "g",
		Consumer: "c2",
		Messages: msgIDs,
	}).Result()
	if err != nil || len(claimed) != 2 {
		t.Fatalf("xclaim: %+v %v", claimed, err)
	}
	// The PEL must now belong to c2.
	ext, _ := c.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: "s", Group: "g", Start: "-", End: "+", Count: 10,
	}).Result()
	claimedSet := map[string]bool{msgIDs[0]: true, msgIDs[1]: true}
	for _, row := range ext {
		want := "c1"
		if claimedSet[row.ID] {
			want = "c2"
		}
		if row.Consumer != want {
			t.Errorf("owner of %s: want %s, got %s", row.ID, want, row.Consumer)
		}
	}

	// XINFO GROUPS.
	groups, err := c.XInfoGroups(ctx, "s").Result()
	if err != nil || len(groups) != 1 || groups[0].Name != "g" || groups[0].Pending != 4 {
		t.Fatalf("xinfogroups: %+v %v", groups, err)
	}
}

func mustID(i int) string {
	return string(rune('0'+i)) + "-1"
}

// TestBlockingCommands covers BZPOPMIN and BRPOPLPUSH end to end.
func TestBlockingCommands(t *testing.T) {
	c, addr := newClientOn(t)
	ctx := context.Background()
	other := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2})
	defer other.Close()

	// BZPOPMIN served by a later ZADD.
	go func() {
		time.Sleep(50 * time.Millisecond)
		other.ZAdd(ctx, "bz", redis.Z{Score: 7, Member: "late"})
	}()
	res, err := c.BZPopMin(ctx, 2*time.Second, "bz").Result()
	if err != nil || res.Member != "late" || res.Score != 7 {
		t.Fatalf("bzpopmin: %+v %v", res, err)
	}

	// BRPOPLPUSH moves the element and replies it.
	go func() {
		time.Sleep(50 * time.Millisecond)
		other.RPush(ctx, "src", "job")
	}()
	got, err := c.BRPopLPush(ctx, "src", "dst", 2*time.Second).Result()
	if err != nil || got != "job" {
		t.Fatalf("brpoplpush: %q %v", got, err)
	}
	if l, _ := c.LLen(ctx, "dst").Result(); l != 1 {
		t.Fatalf("destination list: %d", l)
	}

	// Both time out with redis.Nil on empty keys.
	if _, err := c.BZPopMin(ctx, 100*time.Millisecond, "empty").Result(); err != redis.Nil {
		t.Fatalf("bzpopmin timeout: %v", err)
	}
	if _, err := c.BLPop(ctx, 100*time.Millisecond, "empty2").Result(); err != redis.Nil {
		t.Fatalf("blpop timeout: %v", err)
	}
}
