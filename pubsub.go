package redistore

import (
	"strings"
)

// Pub/Sub commands.
//
// The subscription registry is redcon's: it already handles the fiddly part,
// which is detaching the connection from the command loop and serving it from a
// dedicated goroutine once a client is in subscriber mode.

func cmdSubscribe(c *Ctx, args [][]byte) error  { return subscribeCmd(c, args, false) }
func cmdPSubscribe(c *Ctx, args [][]byte) error { return subscribeCmd(c, args, true) }

func subscribeCmd(c *Ctx, args [][]byte, pattern bool) error {
	if len(args) < 1 {
		return WrongArgs("subscribe")
	}
	srv, conn := c.pubsubTarget()
	if srv == nil || conn == nil {
		return &protoError{"ERR SUBSCRIBE is only available on a server connection"}
	}
	for _, a := range args {
		srv.subscribe(conn, string(a), pattern)
	}
	// redcon writes the subscribe confirmations itself, and takes ownership of
	// the connection from here on.
	return nil
}

func cmdUnsubscribe(c *Ctx, args [][]byte) error  { return unsubscribeCmd(c, args, false) }
func cmdPUnsubscribe(c *Ctx, args [][]byte) error { return unsubscribeCmd(c, args, true) }

// unsubscribeCmd only runs outside subscriber mode. Once a client is
// subscribed, redcon's detached-connection loop owns it and answers
// UNSUBSCRIBE / PUNSUBSCRIBE itself.
func unsubscribeCmd(c *Ctx, args [][]byte, pattern bool) error {
	kind := "unsubscribe"
	if pattern {
		kind = "punsubscribe"
	}
	for _, a := range args {
		c.Server().unsubscribeCount(string(a), pattern)
		c.w.WriteArray(3)
		c.w.WriteBulkString(kind)
		c.w.WriteBulkString(string(a))
		c.writeInt(0)
	}
	if len(args) == 0 {
		c.w.WriteArray(3)
		c.w.WriteBulkString(kind)
		c.writeNull()
		c.writeInt(0)
	}
	return nil
}

func cmdPublish(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	srv, _ := c.pubsubTarget()
	if srv == nil {
		c.writeInt(0)
		return nil
	}
	c.writeInt(int64(srv.ps.Publish(string(args[0]), string(args[1]))))
	return nil
}

// cmdPubSub implements PUBSUB CHANNELS / NUMSUB / NUMPAT against the server's
// own subscriber bookkeeping, which redcon's registry does not expose.
func cmdPubSub(c *Ctx, args [][]byte) error {
	if len(args) == 0 {
		return WrongArgs("pubsub")
	}
	srv := c.Server()
	if srv == nil {
		c.w.WriteArray(0)
		return nil
	}
	switch strings.ToUpper(string(args[0])) {
	case "CHANNELS":
		pattern := ""
		if len(args) > 1 {
			pattern = string(args[1])
		}
		names := srv.channelNames(pattern)
		c.w.WriteArray(len(names))
		for _, n := range names {
			c.w.WriteBulkString(n)
		}
		return nil
	case "NUMSUB":
		counts := srv.channelCounts(byteSliceToStrings(args[1:]))
		c.w.WriteArray(len(counts) * 2)
		for _, cc := range counts {
			c.w.WriteBulkString(cc.name)
			c.writeInt(int64(cc.n))
		}
		return nil
	case "NUMPAT":
		c.writeInt(int64(srv.patternCount()))
		return nil
	default:
		return ErrSyntax
	}
}
