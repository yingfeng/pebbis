package redistore

import "strings"

// XGROUP and XINFO are subcommand dispatchers.

func cmdXGroup(c *Ctx, args [][]byte) error {
	if len(args) < 2 {
		return WrongArgs("xgroup")
	}
	sub := args[0]
	rest := args[1:]
	switch strings.ToUpper(string(sub)) {
	case "CREATE":
		return cmdXGroupCreate(c, rest)
	case "DESTROY":
		return cmdXGroupDestroy(c, rest)
	case "CREATECONSUMER":
		return cmdXGroupCreateConsumer(c, rest)
	case "DELCONSUMER":
		return cmdXGroupDelConsumer(c, rest)
	case "SETID":
		return cmdXGroupSetID(c, rest)
	default:
		return ErrSyntax
	}
}

// cmdXGroupSetID implements `XGROUP SETID key group id [ENTRIESREAD n]`. It
// moves the group's last-delivered ID (and optionally the entries-read
// counter); "-" resets to the stream's beginning, matching Redis.
func cmdXGroupSetID(c *Ctx, args [][]byte) error {
	if len(args) < 3 {
		return WrongArgs("xgroup setid")
	}
	key, group := string(args[0]), string(args[1])
	idStr := string(args[2])
	var id streamID
	if idStr == "-" {
		id = streamID{}
	} else {
		parsed, err := parseStreamID(idStr)
		if err != nil {
			return err
		}
		id = parsed
	}
	if _, exists, err := c.Store.groupGet(c.DB, key, group); err != nil {
		return err
	} else if !exists {
		return &protoError{"NOGROUP No such key '" + key + "' or consumer group '" + group + "'"}
	}
	if err := c.Store.groupSet(c.DB, key, group, id); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

func cmdXInfo(c *Ctx, args [][]byte) error {
	if len(args) < 2 {
		return WrongArgs("xinfo")
	}
	switch strings.ToUpper(string(args[0])) {
	case "GROUPS":
		return cmdXInfoGroups(c, args[1:])
	case "STREAM":
		return cmdXInfoStream(c, args[1:])
	default:
		return ErrSyntax
	}
}

// cmdXInfoStream reports the basics: length and last-generated ID.
func cmdXInfoStream(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	key := string(args[0])
	var n int64
	prefix := streamEntryPrefix(c.DB, key)
	_ = c.Store.eng.ScanKeys(prefix, func([]byte) error {
		n++
		return nil
	})
	last, ok, _ := c.Store.lastStreamEntryID(c.DB, key)
	lastStr := "0-0"
	if ok {
		lastStr = last.String()
	}
	c.w.WriteArray(10)
	c.w.WriteBulkString("length")
	c.writeInt(n)
	c.w.WriteBulkString("radix-tree-keys")
	c.writeInt(n)
	c.w.WriteBulkString("radix-tree-nodes")
	c.writeInt(n)
	c.w.WriteBulkString("last-generated-id")
	c.w.WriteBulkString(lastStr)
	c.w.WriteBulkString("groups")
	c.writeInt(int64(xGroupCount(c.Store, c.DB, key)))
	return nil
}

// xGroupCount counts the groups registered on a stream.
func xGroupCount(s *Store, db uint16, key string) int {
	gp := streamGroupPrefix(db, key, "")
	var n int
	_ = s.eng.ScanKeys(gp, func([]byte) error {
		n++
		return nil
	})
	return n
}
