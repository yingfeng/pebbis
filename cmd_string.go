package redistore

import (
	"strconv"
	"strings"
	"time"

	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/storage"
)

// String commands.

func cmdGet(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	val, ok, err := c.Store.getString(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok {
		c.writeNull()
		return nil
	}
	c.writeBulk(val)
	return nil
}

func cmdSet(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	if err := c.checkBulkLen(args[0], args[1]); err != nil {
		return err
	}
	opt, err := parseSetOption(args, 2)
	if err != nil {
		return err
	}
	old, _, written, err := c.Store.setString(c.DB, string(args[0]), args[1], opt)
	if err != nil {
		return err
	}
	if !written {
		// NX/XX rejected the write. With GET the old value is still returned.
		if opt.GET {
			c.writeBulk(old)
			return nil
		}
		c.writeNull()
		return nil
	}
	if opt.GET {
		c.writeBulk(old)
		return nil
	}
	c.writeOK()
	return nil
}

func cmdSetNX(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	opt := SetOption{NX: true}
	_, _, written, err := c.Store.setString(c.DB, string(args[0]), args[1], opt)
	if err != nil {
		return err
	}
	c.writeInt(btoi(written))
	return nil
}

func cmdSetXX(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	opt := SetOption{XX: true}
	_, _, written, err := c.Store.setString(c.DB, string(args[0]), args[1], opt)
	if err != nil {
		return err
	}
	c.writeInt(btoi(written))
	return nil
}

func cmdSetEx(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	secs, err := toInt64(args[1])
	if err != nil {
		return err
	}
	if secs <= 0 {
		return ErrInvalidExpire
	}
	opt := SetOption{EX: time.Duration(secs) * time.Second}
	if _, _, _, err := c.Store.setString(c.DB, string(args[0]), args[2], opt); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

func cmdPSetEx(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	ms, err := toInt64(args[1])
	if err != nil {
		return err
	}
	if ms <= 0 {
		return ErrInvalidExpire
	}
	opt := SetOption{PX: time.Duration(ms) * time.Millisecond}
	if _, _, _, err := c.Store.setString(c.DB, string(args[0]), args[2], opt); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

func cmdGetSet(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	old, _, _, err := c.Store.setString(c.DB, string(args[0]), args[1], SetOption{GET: true})
	if err != nil {
		return err
	}
	c.writeBulk(old)
	return nil
}

func cmdGetDel(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	val, ok, err := c.Store.getString(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok {
		c.writeNull()
		return nil
	}
	if _, err := c.Store.deleteKey(c.DB, string(args[0])); err != nil {
		return err
	}
	c.writeBulk(val)
	return nil
}

func cmdMSet(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	if len(args)%2 != 0 {
		return WrongArgs("mset")
	}
	if err := c.Store.mSetPairs(c.DB, args); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

// mSetPairs is the shared MSET implementation.
//
// One batch keeps MSET atomic on the durable side; the index is updated
// afterwards, which is safe because Pebble is the source of truth.
func (s *Store) mSetPairs(db uint16, args [][]byte) error {
	batch := s.eng.Batch()
	defer batch.Close()

	type pair struct {
		key string
		val []byte
	}
	pairs := make([]pair, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		key, val := string(args[i]), args[i+1]
		prevExpire := s.oldExpiry(db, key)
		if err := batch.Set(storage.EncodeDataKey(db, key),
			storage.EncodeValue(config.TypeString, 0, val)); err != nil {
			return err
		}
		if prevExpire != 0 {
			if err := batch.Delete(storage.EncodeExpireKey(prevExpire, db, key)); err != nil {
				return err
			}
		}
		pairs = append(pairs, pair{key, val})
	}
	if err := s.eng.Apply(batch); err != nil {
		return err
	}
	for _, p := range pairs {
		s.dict.Set(db, p.key, config.TypeString, 0, p.val)
	}
	s.evictor.MaybeEvict()
	return nil
}

func cmdMSetNX(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	if len(args)%2 != 0 {
		return WrongArgs("msetnx")
	}
	// Reject the whole command if any key already exists.
	for i := 0; i < len(args); i += 2 {
		if _, _, _, ok, err := c.Store.getTyped(c.DB, string(args[i])); err != nil {
			return err
		} else if ok {
			c.writeInt(0)
			return nil
		}
	}
	// Write via the shared pair writer directly: calling cmdMSet here would
	// emit its own OK reply and desynchronise the response stream.
	if err := c.Store.mSetPairs(c.DB, args); err != nil {
		return err
	}
	c.writeInt(1)
	return nil
}

func cmdMGet(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	c.w.WriteArray(len(args))
	for _, a := range args {
		val, ok, err := c.Store.getString(c.DB, string(a))
		if err != nil {
			if err == ErrWrongType {
				// Redis: non-string keys reply nil, not WRONGTYPE. The array
				// header is already on the wire, so an error here would
				// desynchronise the whole response stream.
				c.writeNull()
				continue
			}
			return err
		}
		if !ok {
			c.writeNull()
			continue
		}
		c.writeBulk(val)
	}
	return nil
}

func cmdAppend(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	key := string(args[0])
	cur, typ, expireAt, ok, err := c.Store.getTyped(c.DB, key)
	if err != nil {
		return err
	}
	if ok && typ != config.TypeString {
		return ErrWrongType
	}
	next := make([]byte, 0, len(cur)+len(args[1]))
	next = append(next, cur...)
	next = append(next, args[1]...)
	// APPEND preserves an existing TTL, like every other value-mutating command.
	if err := c.Store.setStringSimple(c.DB, key, next, expireAt); err != nil {
		return err
	}
	c.writeInt(int64(len(next)))
	return nil
}

func cmdStrLen(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	val, ok, err := c.Store.getString(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok {
		c.writeInt(0)
		return nil
	}
	c.writeInt(int64(len(val)))
	return nil
}

// incrBy implements INCR / DECR / INCRBY / DECRBY.
func incrBy(c *Ctx, key string, delta int64) error {
	cur, typ, expireAt, ok, err := c.Store.getTyped(c.DB, key)
	if err != nil {
		return err
	}
	var n int64
	if ok {
		// A wrong type must be reported before any arithmetic.
		if typ != config.TypeString {
			return ErrWrongType
		}
		n, err = toInt64(cur)
		if err != nil {
			return err
		}
	}
	// Detect overflow the same way Redis does: the sign must not flip.
	if (delta > 0 && n > maxInt64-delta) || (delta < 0 && n < minInt64-delta) {
		return ErrOverflow
	}
	next := n + delta
	if err := c.Store.setStringSimple(c.DB, key, []byte(strconv.FormatInt(next, 10)), expireAt); err != nil {
		return err
	}
	c.writeInt(next)
	return nil
}

const (
	maxInt64 = int64(^uint64(0) >> 1)
	minInt64 = -maxInt64 - 1
)

func cmdIncr(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	return incrBy(c, string(args[0]), 1)
}

func cmdDecr(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	return incrBy(c, string(args[0]), -1)
}

func cmdIncrBy(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	d, err := toInt64(args[1])
	if err != nil {
		return err
	}
	return incrBy(c, string(args[0]), d)
}

func cmdDecrBy(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	d, err := toInt64(args[1])
	if err != nil {
		return err
	}
	if d == minInt64 {
		return ErrOverflow
	}
	return incrBy(c, string(args[0]), -d)
}

func cmdGetRange(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	// Redis validates the indices before looking the key up, so a bad index
	// reports an error even when the key is absent.
	start, err := toInt64(args[1])
	if err != nil {
		return err
	}
	end, err := toInt64(args[2])
	if err != nil {
		return err
	}
	// Redis special case: both indices negative and start after end describe
	// an empty range and must return "" (e.g. GETRANGE k -100 -101).
	if start < 0 && end < 0 && start > end {
		c.w.WriteBulkString("")
		return nil
	}
	val, ok, err := c.Store.getString(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok {
		c.w.WriteBulkString("")
		return nil
	}
	n := int64(len(val))
	start, end = resolveRange(start, end, n)
	if start > end || start >= n {
		c.w.WriteBulkString("")
		return nil
	}
	c.w.WriteBulk(val[start : end+1])
	return nil
}

func cmdSetRange(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	key := string(args[0])
	// Validate the offset before the key lookup, matching Redis.
	offset, err := toInt64(args[1])
	if err != nil {
		return err
	}
	if offset < 0 {
		return &protoError{"ERR offset is out of range"}
	}
	value := args[2]
	cur, typ, expireAt, ok, err := c.Store.getTyped(c.DB, key)
	if err != nil {
		return err
	}
	if ok && typ != config.TypeString {
		return ErrWrongType
	}
	// Redis: SETRANGE on a missing key with an empty value is a no-op that
	// must not create the key and returns 0.
	if !ok && len(value) == 0 {
		c.writeInt(0)
		return nil
	}
	// The result is at least as long as the offset plus the new bytes, but
	// never shorter than the existing value: SETRANGE overwrites in place and
	// keeps whatever tail it did not touch.
	need := offset + int64(len(value))
	if need < int64(len(cur)) {
		need = int64(len(cur))
	}
	if need > maxValueSize {
		return &protoError{"ERR string exceeds maximum allowed size"}
	}
	next := make([]byte, need)
	copy(next, cur)
	copy(next[offset:], value)
	if err := c.Store.setStringSimple(c.DB, key, next, expireAt); err != nil {
		return err
	}
	c.writeInt(need)
	return nil
}

// maxValueSize mirrors Redis' 512 MB string limit.
const maxValueSize = 512 << 20

// resolveRange normalises Redis' inclusive, negative-aware index pair.
func resolveRange(start, end, n int64) (int64, int64) {
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if end >= n {
		end = n - 1
	}
	return start, end
}

func btoi(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// ---------- string: GETEX / DELEX ----------

// cmdGetEx returns a string value and optionally adjusts its TTL.
func cmdGetEx(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	var (
		ex, px, exat, pxat int64
		persist            bool
		kinds              int
	)
	for i := 1; i < len(args); i++ {
		arg := args[i]
		switch {
		case foldEqual(arg, "EX"):
			v, err := nextInt(args, &i, "EX")
			if err != nil {
				return err
			}
			if v <= 0 {
				return ErrInvalidExpire
			}
			ex, kinds = v, kinds+1
		case foldEqual(arg, "PX"):
			v, err := nextInt(args, &i, "PX")
			if err != nil {
				return err
			}
			if v <= 0 {
				return ErrInvalidExpire
			}
			px, kinds = v, kinds+1
		case foldEqual(arg, "EXAT"):
			v, err := nextInt(args, &i, "EXAT")
			if err != nil {
				return err
			}
			if v <= 0 {
				return ErrInvalidExpire
			}
			exat, kinds = v, kinds+1
		case foldEqual(arg, "PXAT"):
			v, err := nextInt(args, &i, "PXAT")
			if err != nil {
				return err
			}
			if v <= 0 {
				return ErrInvalidExpire
			}
			pxat, kinds = v, kinds+1
		case foldEqual(arg, "PERSIST"):
			persist, kinds = true, kinds+1
		default:
			return ErrSyntax
		}
	}
	if kinds > 1 {
		return ErrSyntax
	}
	cur, typ, _, ok, err := c.Store.getTyped(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok {
		c.writeNull()
		return nil
	}
	if typ != config.TypeString {
		return ErrWrongType
	}
	newExpire := int64(0)
	now := c.Store.clock.NowMilli()
	switch {
	case persist:
		newExpire = 0
	case ex > 0:
		newExpire = now + ex*1000
	case px > 0:
		newExpire = now + px
	case exat > 0:
		newExpire = exat * 1000
	case pxat > 0:
		newExpire = pxat
	}
	if kinds > 0 {
		if err := c.Store.setStringSimple(c.DB, string(args[0]), cur, newExpire); err != nil {
			return err
		}
	}
	c.writeBulk(cur)
	return nil
}

// cmdDelex deletes a key, optionally gated on its current string value
// (Redis 8's conditional delete). Without a condition it behaves like DEL.
func cmdDelex(c *Ctx, args [][]byte) error {
	if len(args) < 1 {
		return WrongArgs("delex")
	}
	key := string(args[0])
	var cond string
	var condVal []byte
	rest := args[1:]
	if len(rest) > 0 {
		switch strings.ToUpper(string(rest[0])) {
		case "IFEQ", "IFNE":
			if len(rest) != 2 {
				return WrongArgs("delex")
			}
			cond = strings.ToUpper(string(rest[0]))
			condVal = rest[1]
		default:
			return ErrSyntax
		}
	}
	payload, typ, _, ok, err := c.Store.getTyped(c.DB, key)
	if err != nil {
		return err
	}
	deleted := false
	if ok {
		if cond != "" && typ != config.TypeString {
			return &protoError{"ERR Cannot use IFEQ/IFNE on a key that is not of string type if conditions are used"}
		}
		match := cond == "" ||
			(cond == "IFEQ" && string(payload) == string(condVal)) ||
			(cond == "IFNE" && string(payload) != string(condVal))
		if match {
			if _, err := c.Store.deleteKey(c.DB, key); err != nil {
				return err
			}
			deleted = true
		}
	}
	c.writeInt(btoi(deleted))
	return nil
}
