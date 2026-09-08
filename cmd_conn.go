package pebbis

import "strings"

// Connection management commands.

func cmdPing(c *Ctx, args [][]byte) error {
	if len(args) == 0 {
		c.w.WriteString("PONG")
		return nil
	}
	if len(args) > 1 {
		return WrongArgs("ping")
	}
	c.w.WriteBulk(args[0])
	return nil
}

func cmdEcho(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	c.w.WriteBulk(args[0])
	return nil
}

func cmdSelect(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	idx, err := toInt64(args[0])
	if err != nil {
		return err
	}
	if idx < 0 || idx >= int64(c.Store.DBCount()) {
		return ErrDBIndex
	}
	// The server reads DB back off the Ctx after the handler returns and
	// stores it on the connection.
	c.DB = uint16(idx)
	c.writeOK()
	return nil
}

func cmdQuit(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	c.writeOK()
	return ErrQuit
}

// cmdCommand implements COMMAND COUNT / COMMAND LIST. A full COMMAND reply is
// not needed for clients to work; COUNT is what most of them probe with.
func cmdCommand(c *Ctx, args [][]byte) error {
	if len(args) == 0 {
		c.w.WriteArray(0)
		return nil
	}
	switch strings.ToLower(string(args[0])) {
	case "count":
		c.w.WriteInt(CommandCount())
	case "list":
		names := CommandNames()
		c.w.WriteArray(len(names))
		for _, n := range names {
			c.w.WriteBulkString(n)
		}
	default:
		c.w.WriteArray(0)
	}
	return nil
}

// cmdHello handles RESP3 negotiation. Only RESP2 is implemented, so HELLO 3 is
// answered with an error, which every mainstream client treats as "fall back to
// RESP2".
func cmdHello(c *Ctx, args [][]byte) error {
	if len(args) > 0 && strings.ToLower(string(args[0])) == "3" {
		return &protoError{"NOPROTO unsupported protocol version"}
	}
	// Minimal RESP2 HELLO reply: server name, version, proto.
	c.w.WriteArray(4)
	c.w.WriteBulkString("server")
	c.w.WriteBulkString("pebbis")
	c.w.WriteBulkString("proto")
	c.w.WriteInt(2)
	return nil
}
