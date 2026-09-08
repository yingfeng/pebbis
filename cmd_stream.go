package pebbis

import (
	"strconv"
	"strings"
	"time"
)

// Stream commands: the reliable-queue surface. Entries are delivered to a
// consumer group's PEL on read and stay there until XACK, so a crash between
// delivery and processing loses nothing.

// parseStreamBounds reads the start/end of XRANGE: "-" / "+" / id / ms.
func parseStreamBound(b []byte, asStart bool) (streamID, error) {
	s := string(b)
	exclusive := false
	if strings.HasPrefix(s, "(") {
		// Redis exclusive bound: "(id" excludes the ID itself. "(" must be
		// followed by a valid ID; "(-" and "(0-0" as a start are errors.
		exclusive = true
		s = s[1:]
	}
	switch s {
	case "-":
		if exclusive {
			return streamID{}, &protoError{"ERR Invalid stream ID specified as stream command argument"}
		}
		return streamID{}, nil
	case "+":
		if exclusive {
			return streamID{}, &protoError{"ERR Invalid stream ID specified as stream command argument"}
		}
		return streamID{ms: ^uint64(0), seq: ^uint64(0)}, nil
	}
	id, err := parseStreamID(s)
	if err != nil {
		return id, err
	}
	if exclusive {
		if asStart {
			// "(1-0" starts after 1-0.
			if id.seq == ^uint64(0) {
				return streamID{ms: id.ms + 1}, nil
			}
			return streamID{ms: id.ms, seq: id.seq + 1}, nil
		}
		// "(1-2" ends before 1-2.
		if id.seq == 0 {
			if id.ms == 0 {
				return streamID{}, &protoError{"ERR Invalid stream ID specified as stream command argument"}
			}
			return streamID{ms: id.ms - 1, seq: ^uint64(0)}, nil
		}
		return streamID{ms: id.ms, seq: id.seq - 1}, nil
	}
	// Incomplete IDs pad differently per side: XSTART 1 means 1-0, XEND 1
	// means 1-max.
	if asStart && !strings.Contains(s, "-") {
		return id, nil
	}
	if !asStart && !strings.Contains(s, "-") {
		return streamID{ms: id.ms, seq: ^uint64(0)}, nil
	}
	return id, nil
}

func writeStreamEntries(c *Ctx, entries []streamEntry) {
	c.w.WriteArray(len(entries))
	for _, e := range entries {
		c.w.WriteArray(2)
		c.w.WriteBulkString(e.id.String())
		c.w.WriteArray(len(e.fields) * 2)
		for _, f := range e.fields {
			c.w.WriteBulkString(f.Field)
			c.w.WriteBulkString(f.Value)
		}
	}
}

func cmdXAdd(c *Ctx, args [][]byte) error {
	if len(args) < 3 {
		return WrongArgs("xadd")
	}
	key := string(args[0])
	i := 1
	nomkstream := false
	if strings.EqualFold(string(args[i]), "NOMKSTREAM") {
		nomkstream = true
		i++
	}
	// Optional inline trimming, e.g. `XADD key MAXLEN ~ 1000 id f v`.
	maxlen := -1
	var minid streamID
	trimMinID := false
	if i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "MAXLEN":
			i++
			for i < len(args) && (strings.EqualFold(string(args[i]), "~") || strings.EqualFold(string(args[i]), "=")) {
				i++ // approximate/exact markers: executed exactly either way
			}
			if i >= len(args) {
				return ErrSyntax
			}
			n, err := atoi(args[i])
			if err != nil || n < 0 {
				return ErrNotInteger
			}
			maxlen = n
			i++
		case "MINID":
			i++
			for i < len(args) && (strings.EqualFold(string(args[i]), "~") || strings.EqualFold(string(args[i]), "=")) {
				i++
			}
			if i >= len(args) {
				return ErrSyntax
			}
			parsed, err := parseStreamID(string(args[i]))
			if err != nil {
				return err
			}
			minid = parsed
			trimMinID = true
			i++
		}
		if i+1 < len(args) && strings.EqualFold(string(args[i]), "LIMIT") {
			i += 2 // accepted for compatibility; trimming is exact here
		}
	}
	if i >= len(args) {
		return WrongArgs("xadd")
	}
	var id streamID
	auto := string(args[i]) == "*"
	partialAuto := false
	var partialMS uint64
	idStr := string(args[i])
	if !auto && strings.HasSuffix(idStr, "-*") {
		// "ms-*" asks the server to auto-generate the sequence part while
		// keeping the caller-supplied millisecond part fixed.
		msStr := idStr[:len(idStr)-2]
		v, perr := strconv.ParseUint(msStr, 10, 64)
		if perr != nil {
			return &protoError{"ERR Invalid stream ID specified as stream command argument"}
		}
		partialAuto = true
		partialMS = v
		i++
	} else if !auto {
		// A bare millisecond part ("152...") defaults the sequence to 0,
		// matching Redis.
		var err error
		if id, err = parseStreamID(idStr); err != nil {
			return err
		}
		if id.ms == 0 && id.seq == 0 {
			return &protoError{"ERR The ID specified in XADD must be greater than 0-0"}
		}
		i++
	} else {
		i++
	}
	if (len(args)-i)%2 != 0 {
		return &protoError{"ERR wrong number of arguments for XADD: expected an even number of field-value pairs"}
	}
	fields := make([]streamField, 0, (len(args)-i)/2)
	for ; i < len(args); i += 2 {
		fields = append(fields, streamField{Field: string(args[i]), Value: string(args[i+1])})
	}

	stored, err := c.Store.xAdd(c.DB, key, id, auto, partialAuto, partialMS, fields, nomkstream)
	if err != nil {
		return err
	}
	if maxlen >= 0 {
		if _, err := c.Store.xTrimMaxlen(c.DB, key, maxlen); err != nil {
			return err
		}
	}
	if trimMinID {
		if _, err := c.Store.xTrimMinID(c.DB, key, minid); err != nil {
			return err
		}
	}
	if nomkstream && stored.equal(streamID{}) {
		c.writeNull()
		return nil
	}
	c.w.WriteBulkString(stored.String())
	return nil
}

func cmdXLen(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	var n int64
	prefix := streamEntryPrefix(c.DB, string(args[0]))
	_ = c.Store.eng.ScanKeys(prefix, func([]byte) error {
		n++
		return nil
	})
	c.writeInt(n)
	return nil
}

func cmdXRange(c *Ctx, args [][]byte) error    { return xRangeCmd(c, args, false) }
func cmdXRevRange(c *Ctx, args [][]byte) error { return xRangeCmd(c, args, true) }

func xRangeCmd(c *Ctx, args [][]byte, rev bool) error {
	if len(args) < 3 {
		return WrongArgs("xrange")
	}
	start, err := parseStreamBound(args[1], !rev)
	if err != nil {
		return err
	}
	end, err := parseStreamBound(args[2], rev)
	if err != nil {
		return err
	}
	count := 0
	if rev && len(args) > 3 {
		if len(args) != 5 || !strings.EqualFold(string(args[3]), "COUNT") {
			return ErrSyntax
		}
		if count, err = atoi(args[4]); err != nil {
			return err
		}
	} else if !rev && len(args) > 3 {
		if len(args) != 5 || !strings.EqualFold(string(args[3]), "COUNT") {
			return ErrSyntax
		}
		if count, err = atoi(args[4]); err != nil {
			return err
		}
	}
	entries, err := c.Store.xRange(c.DB, string(args[0]), start, end, count, rev)
	if err != nil {
		return err
	}
	writeStreamEntries(c, entries)
	return nil
}

func atoi(b []byte) (int, error) {
	n := 0
	neg := false
	for i, ch := range b {
		if i == 0 && ch == '-' {
			neg = true
			continue
		}
		if ch < '0' || ch > '9' {
			return 0, ErrNotInteger
		}
		n = n*10 + int(ch-'0')
	}
	if neg {
		n = -n
	}
	return n, nil
}

func cmdXDel(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	ids := make([]streamID, 0, len(args)-1)
	for _, a := range args[1:] {
		id, err := parseStreamID(string(a))
		if err != nil {
			return err
		}
		ids = append(ids, id)
	}
	n, err := c.Store.xDel(c.DB, string(args[0]), ids)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdXTrim(c *Ctx, args [][]byte) error {
	if len(args) < 3 {
		return WrongArgs("xtrim")
	}
	switch strings.ToUpper(string(args[1])) {
	case "MAXLEN":
		approx := false
		idx := 2
		switch strings.ToUpper(string(args[idx])) {
		case "~":
			approx = true
			idx++
		case "=":
			// Exact limit marker; executed like the approximate form here.
			idx++
		}
		n, err := atoi(args[idx])
		if err != nil || n < 0 {
			return ErrNotInteger
		}
		// The approximate form is executed exactly: correctness first, the
		// node-count optimisation is a pure latency trade we do not need yet.
		_ = approx
		r, err := c.Store.xTrimMaxlen(c.DB, string(args[0]), n)
		if err != nil {
			return err
		}
		c.writeInt(r)
		return nil
	case "MINID":
		id, err := parseStreamID(string(args[2]))
		if err != nil {
			return err
		}
		r, err := c.Store.xTrimMinID(c.DB, string(args[0]), id)
		if err != nil {
			return err
		}
		c.writeInt(r)
		return nil
	default:
		return ErrSyntax
	}
}

// parseStreamsTail reads the STREAMS key... id... tail of XREAD / XREADGROUP.
// Every key gets exactly one ID.
func parseStreamsTail(c *Ctx, args [][]byte, off int) ([]string, []streamID, error) {
	rest := args[off:]
	if len(rest) < 3 || !strings.EqualFold(string(rest[0]), "STREAMS") {
		return nil, nil, ErrSyntax
	}
	rest = rest[1:]
	ids := len(rest) / 2
	if ids < 1 || len(rest)%2 != 0 {
		return nil, nil, ErrSyntax
	}
	keys := byteSliceToStrings(rest[:ids])
	out := make([]streamID, 0, ids)
	for ki, a := range rest[ids:] {
		switch string(a) {
		case ">":
			out = append(out, streamID{ms: ^uint64(0), seq: ^uint64(0)}) // marker, XREADGROUP only
		case "$":
			// "$" is the ID of the stream's last entry; reading from it
			// returns only entries strictly newer than what exists now.
			if last, ok, err := c.Store.lastStreamEntryID(c.DB, keys[ki]); err != nil {
				return nil, nil, err
			} else if ok {
				out = append(out, last)
			} else {
				out = append(out, streamID{})
			}
		default:
			id, err := parseStreamID(string(a))
			if err != nil {
				return nil, nil, err
			}
			out = append(out, id)
		}
	}
	return keys, out, nil
}

func cmdXRead(c *Ctx, args [][]byte) error {
	if len(args) < 3 {
		return WrongArgs("xread")
	}
	count, blockMs := 0, int64(-1)
	i := 0
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "COUNT":
			if i+1 >= len(args) {
				return ErrSyntax
			}
			v, err := atoi(args[i+1])
			if err != nil {
				return err
			}
			count = v
			i += 2
		case "BLOCK":
			if i+1 >= len(args) {
				return ErrSyntax
			}
			v, err := toInt64(args[i+1])
			if err != nil || v < 0 {
				return &protoError{"ERR timeout is negative"}
			}
			blockMs = v
			i += 2
		default:
			goto streams
		}
	}
streams:
	keys, ids, err := parseStreamsTail(c, args, i)
	if err != nil {
		return err
	}

	timeout := time.Duration(blockMs) * time.Millisecond
	deadline := time.Time{}
	if blockMs >= 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		bvkeysVersion := c.Store.blockVersion()
		var reply []string
		var batches [][]streamEntry
		for ki, k := range keys {
			entries, err := c.Store.xRange(c.DB, k, nextID(ids[ki]), streamID{ms: ^uint64(0), seq: ^uint64(0)}, count, false)
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				continue
			}
			reply = append(reply, k)
			batches = append(batches, entries)
		}
		if len(reply) > 0 {
			c.w.WriteArray(len(reply))
			for bi := range batches {
				c.w.WriteArray(2)
				c.w.WriteBulkString(reply[bi])
				writeStreamEntries(c, batches[bi])
			}
			return nil
		}
		if blockMs < 0 {
			c.writeNull()
			return nil
		}
		if blockMs > 0 {
			if remaining := time.Until(deadline); remaining <= 0 {
				c.writeNull()
				return nil
			} else {
				timeout = remaining
			}
		}
		if !c.blockSleep(bvkeysVersion, timeout) {
			c.writeNull()
			return nil
		}
		bvkeysVersion = c.Store.blockVersion()
		// After a wake the caller wants everything newer than what it asked
		// for; the IDs stay as the low bound, which is exactly that.
	}
}

func cmdXGroupCreate(c *Ctx, args [][]byte) error {
	if len(args) < 3 {
		return WrongArgs("xgroup")
	}
	key, group := string(args[0]), string(args[1])
	idStr := string(args[2])
	mkstream := len(args) > 3 && strings.EqualFold(string(args[3]), "MKSTREAM")

	if _, exists, err := c.Store.groupGet(c.DB, key, group); err != nil {
		return err
	} else if exists {
		return &protoError{"BUSYGROUP consumer group name '" + group + "' already exists"}
	}

	// XGROUP CREATE requires the stream to already exist unless MKSTREAM is
	// supplied. An absent stream (no entries) is an error, matching Redis.
	if !mkstream {
		if _, ok, err := c.Store.lastStreamEntryID(c.DB, key); err != nil {
			return err
		} else if !ok {
			return &protoError{"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically."}
		}
	}

	if idStr == "$" {
		// "$" means "start from the stream's last entry" - which on an empty
		// stream is 0-0, so everything added afterwards is new.
		last, ok, err := c.Store.lastStreamEntryID(c.DB, key)
		if err != nil {
			return err
		}
		if !ok {
			last = streamID{}
		}
		if err := c.Store.groupSet(c.DB, key, group, last); err != nil {
			return err
		}
		c.writeOK()
		return nil
	}
	id, err := parseStreamID(idStr)
	if err != nil {
		return err
	}
	if err := c.Store.groupSet(c.DB, key, group, id); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

func cmdXGroupDestroy(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	if err := c.Store.groupDestroy(c.DB, string(args[0]), string(args[1])); err != nil {
		return err
	}
	c.writeInt(1)
	return nil
}

func cmdXGroupCreateConsumer(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	// Consumers materialise in the PEL on first delivery; creation is a no-op
	// that must still report success.
	c.writeInt(1)
	return nil
}

func cmdXGroupDelConsumer(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	key, group, consumer := string(args[0]), string(args[1]), string(args[2])
	pending, err := c.Store.pelList(c.DB, key, group, consumer, streamID{}, streamID{ms: ^uint64(0), seq: ^uint64(0)}, 0)
	if err != nil {
		return err
	}
	batch := c.Store.eng.Batch()
	defer batch.Close()
	for _, p := range pending {
		if err := batch.Delete(appendStreamPELKey(nil, c.DB, key, group, p.id)); err != nil {
			return err
		}
	}
	if err := c.Store.eng.Apply(batch); err != nil {
		return err
	}
	c.writeInt(int64(len(pending)))
	return nil
}

func cmdXReadGroup(c *Ctx, args [][]byte) error {
	if len(args) < 6 {
		return WrongArgs("xreadgroup")
	}
	if !strings.EqualFold(string(args[0]), "GROUP") {
		return ErrSyntax
	}
	group, consumer := string(args[1]), string(args[2])
	count, blockMs := 0, int64(-1)
	i := 3
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "COUNT":
			if i+1 >= len(args) {
				return ErrSyntax
			}
			v, err := atoi(args[i+1])
			if err != nil {
				return err
			}
			count = v
			i += 2
		case "BLOCK":
			if i+1 >= len(args) {
				return ErrSyntax
			}
			v, err := toInt64(args[i+1])
			if err != nil || v < 0 {
				return &protoError{"ERR timeout is negative"}
			}
			blockMs = v
			i += 2
		default:
			goto streams
		}
	}
streams:
	keys, ids, err := parseStreamsTail(c, args, i)
	if err != nil {
		return err
	}
	// A key that exists as a non-stream type is a WRONGTYPE error, matching Redis.
	for _, k := range keys {
		if _, _, _, ok, _ := c.Store.getTyped(c.DB, k); ok {
			if _, isStream, _ := c.Store.lastStreamEntryID(c.DB, k); !isStream {
				return ErrWrongType
			}
		}
	}

	newOnly := len(ids) > 0 && ids[0].equal(streamID{ms: ^uint64(0), seq: ^uint64(0)})

	var reply []string
	var batches [][]streamEntry
	if !newOnly {
		// History mode: hand back this consumer's pending entries.
		for _, k := range keys {
			entries, err := c.Store.pelOwned(c.DB, k, group, consumer, count)
			if err != nil {
				return err
			}
			// History mode replies with the stream even when the PEL is empty
			// ([[stream, []]]), matching Redis; only ">" mode omits idle keys.
			reply = append(reply, k)
			batches = append(batches, entries)
		}
	} else {
		for _, k := range keys {
			last, found, err := c.Store.groupGet(c.DB, k, group)
			if err != nil {
				return err
			}
			if !found {
				return &protoError{"NOGROUP No such consumer group '" + group + "' for key name '" + k + "'"}
			}
			entries, err := c.Store.xRange(c.DB, k, nextID(last), streamID{ms: ^uint64(0), seq: ^uint64(0)}, count, false)
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				continue
			}
			for _, e := range entries {
				if err := c.Store.pelAdd(c.DB, k, group, consumer, e.id); err != nil {
					return err
				}
			}
			if err := c.Store.groupSet(c.DB, k, group, entries[len(entries)-1].id); err != nil {
				return err
			}
			reply = append(reply, k)
			batches = append(batches, entries)
		}
	}

	if len(reply) > 0 {
		c.w.WriteArray(len(reply))
		for bi := range batches {
			c.w.WriteArray(2)
			c.w.WriteBulkString(reply[bi])
			writeStreamEntries(c, batches[bi])
		}
		return nil
	}
	if blockMs < 0 {
		c.writeNull()
		return nil
	}
	// Blocking with ">": wait for new entries on any of the keys, then serve.
	if newOnly {
		deadline := time.Time{}
		if blockMs > 0 {
			deadline = time.Now().Add(time.Duration(blockMs) * time.Millisecond)
		}
		for {
			bvkeysVersion := c.Store.blockVersion()
			var t time.Duration
			if blockMs > 0 {
				remaining := time.Until(deadline)
				if remaining <= 0 {
					c.writeNull()
					return nil
				}
				t = remaining
			}
			if !c.blockSleep(bvkeysVersion, t) {
				c.writeNull()
				return nil
			}
			bvkeysVersion = c.Store.blockVersion()
			// Retry the new-entry read once woken.
			var r2 []string
			var b2 [][]streamEntry
			for _, k := range keys {
				last, _, err := c.Store.groupGet(c.DB, k, group)
				if err != nil {
					return err
				}
				entries, err := c.Store.xRange(c.DB, k, nextID(last), streamID{ms: ^uint64(0), seq: ^uint64(0)}, count, false)
				if err != nil {
					return err
				}
				if len(entries) == 0 {
					continue
				}
				for _, e := range entries {
					if err := c.Store.pelAdd(c.DB, k, group, consumer, e.id); err != nil {
						return err
					}
				}
				if err := c.Store.groupSet(c.DB, k, group, entries[len(entries)-1].id); err != nil {
					return err
				}
				r2 = append(r2, k)
				b2 = append(b2, entries)
			}
			if len(r2) > 0 {
				c.w.WriteArray(len(r2))
				for bi := range b2 {
					c.w.WriteArray(2)
					c.w.WriteBulkString(r2[bi])
					writeStreamEntries(c, b2[bi])
				}
				return nil
			}
		}
	}
	c.writeNull()
	return nil
}

func cmdXAck(c *Ctx, args [][]byte) error {
	if len(args) < 3 {
		return WrongArgs("xack")
	}
	key, group := string(args[0]), string(args[1])
	if _, exists, err := c.Store.groupGet(c.DB, key, group); err != nil {
		return err
	} else if !exists {
		return &protoError{"NOGROUP No such key '" + key + "' or consumer group '" + group + "'"}
	}
	var n int64
	for _, a := range args[2:] {
		id, err := parseStreamID(string(a))
		if err != nil {
			return err
		}
		if err := c.Store.pelRemove(c.DB, key, group, id); err != nil {
			return err
		}
		n++
	}
	c.writeInt(n)
	return nil
}

func cmdXPending(c *Ctx, args [][]byte) error {
	if len(args) < 2 {
		return WrongArgs("xpending")
	}
	key, group := string(args[0]), string(args[1])
	start, end := streamID{}, streamID{ms: ^uint64(0), seq: ^uint64(0)}
	consumer := ""
	count := 0
	if len(args) >= 3 && strings.EqualFold(string(args[2]), "IDLE") {
		if len(args) < 4 {
			return ErrSyntax
		}
		args = args[:2]
	}
	if len(args) >= 6 && strings.EqualFold(string(args[2]), "-") || len(args) >= 5 {
		// Summary-vs-detail discrimination: after the group, a "-" begins the
		// detailed form "start end count [consumer]".
	}
	if len(args) >= 5 {
		var err error
		// parseStreamBound understands "-", "+", plain IDs and Redis exclusive
		// "(id" bounds, matching XRANGE semantics.
		if start, err = parseStreamBound(args[2], true); err != nil {
			return err
		}
		if end, err = parseStreamBound(args[3], false); err != nil {
			return err
		}
		v, err := atoi(args[4])
		if err != nil {
			return err
		}
		count = v
		if len(args) >= 6 {
			consumer = string(args[5])
		}
	}
	pending, err := c.Store.pelList(c.DB, key, group, consumer, start, end, count)
	if err != nil {
		return err
	}
	if len(args) < 5 {
		// Summary form: [total, first-id, last-id, [[consumer, count]...]].
		// Missing IDs reply null, matching Redis.
		c.w.WriteArray(4)
		c.writeInt(int64(len(pending)))
		if len(pending) == 0 {
			c.writeNull()
			c.writeNull()
		} else {
			c.w.WriteBulkString(pending[0].id.String())
			c.w.WriteBulkString(pending[len(pending)-1].id.String())
		}
		counts := map[string]int64{}
		var order []string
		for _, p := range pending {
			if _, ok := counts[p.consumer]; !ok {
				order = append(order, p.consumer)
			}
			counts[p.consumer]++
		}
		c.w.WriteArray(len(order))
		for _, name := range order {
			c.w.WriteArray(2)
			c.w.WriteBulkString(name)
			c.writeInt(counts[name])
		}
		return nil
	}
	c.w.WriteArray(len(pending))
	for _, p := range pending {
		c.w.WriteArray(4)
		c.w.WriteBulkString(p.id.String())
		c.w.WriteBulkString(p.consumer)
		c.writeInt(p.deliveryMs / 1000)
		c.writeInt(int64(p.deliveryCert))
	}
	return nil
}

func cmdXClaim(c *Ctx, args [][]byte) error {
	if len(args) < 5 {
		return WrongArgs("xclaim")
	}
	key, group, consumer := string(args[0]), string(args[1]), string(args[2])
	if _, err := toInt64(args[3]); err != nil {
		return err
	}
	var out []streamEntry
	for _, a := range args[4:] {
		id, err := parseStreamID(string(a))
		if err != nil {
			return err
		}
		if err := c.Store.pelMoveTo(c.DB, key, group, consumer, id); err != nil {
			return err
		}
		v, release, err := c.Store.eng.Get(appendStreamEntryKey(nil, c.DB, key, id))
		if err != nil {
			continue
		}
		fields, derr := decodeStreamFields(v)
		release()
		if derr != nil {
			continue
		}
		out = append(out, streamEntry{id: id, fields: fields})
	}
	writeStreamEntries(c, out)
	return nil
}

func cmdXInfoGroups(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	key := string(args[0])
	// Namespace = Seg|db|key|0x00: everything from here on is a group key.
	gp := streamGroupPrefix(c.DB, key, "")
	gp = gp[:len(gp)-1] // drop the empty group's terminator, keep key|0x00
	var groups []string
	err := c.Store.eng.ScanKeys(gp, func(k []byte) error {
		raw := k[len(gp):]
		if len(raw) == 0 || raw[len(raw)-1] != 0x00 {
			return nil
		}
		groups = append(groups, string(raw[:len(raw)-1]))
		return nil
	})
	if err != nil {
		return err
	}
	c.w.WriteArray(len(groups))
	for _, g := range groups {
		pending, _ := c.Store.pelList(c.DB, key, g, "", streamID{}, streamID{ms: ^uint64(0), seq: ^uint64(0)}, 0)
		last, _, _ := c.Store.groupGet(c.DB, key, g)
		c.w.WriteArray(6)
		c.w.WriteBulkString("name")
		c.w.WriteBulkString(g)
		c.w.WriteBulkString("consumers")
		c.writeInt(int64(c.Store.xGroupConsumerCount(c.DB, key, g)))
		c.w.WriteBulkString("pending")
		c.writeInt(int64(len(pending)))
		c.w.WriteBulkString("last-delivered-id")
		c.w.WriteBulkString(last.String())
		c.w.WriteBulkString("entries-read")
		c.writeInt(0)
		c.w.WriteBulkString("lag")
		c.writeInt(0)
	}
	return nil
}
