package redistore

import (
	"strconv"
	"strings"
	"time"

	"github.com/gobwas/glob"
	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/storage"
)

// Generic (key-space) commands.

func cmdDel(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	var n int64
	for _, a := range args {
		key := string(a)
		if _, _, _, ok, err := c.Store.getTyped(c.DB, key); err != nil {
			return err
		} else if !ok {
			continue
		}
		if _, err := c.Store.deleteKey(c.DB, key); err != nil {
			return err
		}
		n++
	}
	c.writeInt(n)
	return nil
}

// cmdUnlink is the non-blocking flavour of DEL. There is no separate free path
// here - the value lives in Pebble - so it behaves identically.
func cmdUnlink(c *Ctx, args [][]byte) error { return cmdDel(c, args) }

func cmdExists(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = string(a)
	}
	n, err := c.Store.exists(c.DB, keys)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdType(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	typ, ok, err := c.Store.typeOf(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok {
		c.w.WriteString("none")
		return nil
	}
	c.w.WriteString(config.TypeName(typ))
	return nil
}

func cmdTTL(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	ms := c.Store.ttlOf(c.DB, string(args[0]))
	switch ms {
	case -1, -2:
		c.writeInt(ms)
	default:
		// Redis rounds TTL up: a key with 1ms left reports 1 second, not 0.
		c.writeInt((ms + 999) / 1000)
	}
	return nil
}

func cmdPTTL(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	c.writeInt(c.Store.ttlOf(c.DB, string(args[0])))
	return nil
}

// expireAtCmd implements EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT.
func expireAtCmd(c *Ctx, key string, at time.Time, opt ExpireOption) error {
	n, err := c.Store.setExpiry(c.DB, key, at, opt)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdExpire(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	secs, err := toInt64(args[1])
	if err != nil {
		return err
	}
	opt, err := parseExpireOption(args, 2)
	if err != nil {
		return err
	}
	return expireAtCmd(c, string(args[0]), c.Store.clock.Now().Add(time.Duration(secs)*time.Second), opt)
}

func cmdPExpire(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	ms, err := toInt64(args[1])
	if err != nil {
		return err
	}
	opt, err := parseExpireOption(args, 2)
	if err != nil {
		return err
	}
	return expireAtCmd(c, string(args[0]), c.Store.clock.Now().Add(time.Duration(ms)*time.Millisecond), opt)
}

func cmdExpireAt(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	ts, err := toInt64(args[1])
	if err != nil {
		return err
	}
	opt, err := parseExpireOption(args, 2)
	if err != nil {
		return err
	}
	return expireAtCmd(c, string(args[0]), time.Unix(ts, 0), opt)
}

func cmdPExpireAt(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	ts, err := toInt64(args[1])
	if err != nil {
		return err
	}
	opt, err := parseExpireOption(args, 2)
	if err != nil {
		return err
	}
	return expireAtCmd(c, string(args[0]), time.UnixMilli(ts), opt)
}

func cmdPersist(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	ok, err := c.Store.persist(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	c.writeInt(btoi(ok))
	return nil
}

func cmdKeys(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	pattern := string(args[0])
	var match func(string) bool
	if pattern == "*" {
		match = func(string) bool { return true }
	} else {
		g, err := glob.Compile(pattern)
		if err != nil {
			return ErrSyntax
		}
		match = g.Match
	}

	// KEYS is O(N) by definition and must not be used in production; scanning
	// Pebble rather than materialising the key list keeps memory flat.
	var out [][]byte
	now := c.Store.clock.NowMilli()
	err := c.Store.eng.ScanKeys(storage.DataPrefix(c.DB), func(k []byte) error {
		key := storage.KeyFromDataKey(k)
		if key == "" {
			return nil
		}
		if !c.Store.keyAlive(c.DB, key, now) {
			return nil
		}
		if match(key) {
			out = append(out, []byte(key))
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.w.WriteArray(len(out))
	for _, k := range out {
		c.w.WriteBulk(k)
	}
	return nil
}

func cmdDBSize(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	c.writeInt(int64(c.Store.dict.Len(c.DB)))
	return nil
}

func cmdFlushDB(c *Ctx, args [][]byte) error {
	if len(args) > 1 {
		return ErrSyntax
	}
	if len(args) == 1 && !strings.EqualFold(string(args[0]), "async") &&
		!strings.EqualFold(string(args[0]), "sync") {
		return ErrSyntax
	}
	if err := c.Store.flushDB(c.DB); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

func cmdFlushAll(c *Ctx, args [][]byte) error {
	if len(args) > 1 {
		return ErrSyntax
	}
	if len(args) == 1 && !strings.EqualFold(string(args[0]), "async") &&
		!strings.EqualFold(string(args[0]), "sync") {
		return ErrSyntax
	}
	for db := range c.Store.DBCount() {
		if err := c.Store.flushDB(uint16(db)); err != nil {
			return err
		}
	}
	c.writeOK()
	return nil
}

func cmdRename(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	if err := c.Store.rename(c.DB, string(args[0]), string(args[1]), false); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

func cmdRenameNX(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	// RENAMENX reports 0 rather than failing when the target exists.
	if _, _, _, exists, err := c.Store.getTyped(c.DB, string(args[1])); err != nil {
		return err
	} else if exists {
		c.writeInt(0)
		return nil
	}
	if err := c.Store.rename(c.DB, string(args[0]), string(args[1]), true); err != nil {
		return err
	}
	c.writeInt(1)
	return nil
}

// cmdScan iterates the keyspace.
//
// The cursor counts how many keys to skip, and iteration follows Pebble's
// stable key order, so a full iteration neither duplicates nor misses keys that
// were present throughout. Stepping is O(cursor) rather than O(1): acceptable
// for an administrative command, and a bucket-ordered O(1) cursor is a
// follow-up.
func cmdScan(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	// The cursor is mandatory; everything after it comes in option/value pairs,
	// so the count is always odd.
	if (len(args)-1)%2 != 0 {
		return ErrSyntax
	}
	cursor, err := strconv.ParseUint(string(args[0]), 10, 64)
	if err != nil {
		return ErrNotInteger
	}
	var matchFn func(string) bool
	count := 10
	for i := 1; i < len(args); i += 2 {
		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			g, cerr := glob.Compile(string(args[i+1]))
			if cerr != nil {
				return ErrSyntax
			}
			matchFn = g.Match
		case "COUNT":
			cnt, perr := strconv.Atoi(string(args[i+1]))
			if perr != nil || cnt <= 0 {
				return ErrSyntax
			}
			count = cnt
		case "TYPE":
			// TYPE filtering needs the aggregate types, which land in P1.
		default:
			return ErrSyntax
		}
	}
	if matchFn == nil {
		matchFn = func(string) bool { return true }
	}

	batch := make([]string, 0, count)
	now := c.Store.clock.NowMilli()
	var seen uint64
	next := cursor

	err = c.Store.eng.ScanKeys(storage.DataPrefix(c.DB), func(k []byte) error {
		if len(batch) >= count {
			return storage.ErrStop
		}
		key := storage.KeyFromDataKey(k)
		if key == "" {
			return nil
		}
		seen++
		// Skip everything already returned on previous calls.
		if seen <= cursor {
			return nil
		}
		next = seen
		if !c.Store.keyAlive(c.DB, key, now) {
			return nil
		}
		if matchFn(key) {
			batch = append(batch, key)
		}
		return nil
	})
	if err != nil && !storage.IsStop(err) {
		return err
	}
	// A batch smaller than COUNT means the iteration ran off the end.
	if len(batch) < count {
		next = 0
	}

	c.w.WriteArray(2)
	c.w.WriteBulkString(strconv.FormatUint(next, 10))
	c.w.WriteArray(len(batch))
	for _, k := range batch {
		c.w.WriteBulkString(k)
	}
	return nil
}

func cmdTouch(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	var n int64
	for _, a := range args {
		if _, _, _, ok, err := c.Store.getTyped(c.DB, string(a)); err != nil {
			return err
		} else if ok {
			n++
		}
	}
	c.writeInt(n)
	return nil
}
