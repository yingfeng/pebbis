package redistore

import (
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"

	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/glob"
	"github.com/redistore/redistore/storage"
)

// Hash commands.

func (a *agg) ensureHash() {
	if a.hash == nil {
		a.hash = map[string][]byte{}
	}
}

// hashSet writes field/value pairs. With nx set, existing fields are left
// alone. It returns how many fields were added.
func (s *Store) hashSet(db uint16, key string, pairs [][2][]byte, nx bool) (int64, error) {
	// A collection already stored sparsely is updated in place: touching one
	// field must not cost a scan of the whole thing.
	if obj, expireAt, exists, err := s.aggEncoding(db, key, config.TypeHash); err != nil {
		return 0, err
	} else if exists && obj.Enc == storage.EncSparse {
		return s.hashSetSparse(db, key, pairs, nx, int64(obj.Count), expireAt)
	}

	a, err := s.loadAgg(db, key, config.TypeHash)
	if err != nil {
		return 0, err
	}
	a.ensureHash()
	var added int64
	for _, p := range pairs {
		if _, exists := a.hash[string(p[0])]; exists {
			if nx {
				continue
			}
		} else {
			added++
		}
		a.hash[string(p[0])] = p[1]
	}
	if err := s.saveAgg(db, key, a); err != nil {
		return 0, err
	}
	return added, nil
}

// hashSetSparse writes only the given fields into a sparse hash.
func (s *Store) hashSetSparse(db uint16, key string, pairs [][2][]byte, nx bool, curCount, expireAt int64) (int64, error) {
	// Per-key lock held by the caller (dispatch) for the whole command.
	seg, _ := storage.SegmentFor(config.TypeHash)
	batch := s.eng.Batch()
	defer batch.Close()

	var added int64
	for _, p := range pairs {
		ek := storage.EncodeElemKey(seg, db, key, string(p[0]))
		_, closer, err := s.eng.Get(ek)
		switch {
		case err == nil:
			closer()
			if nx {
				continue
			}
		case err == storage.ErrNotFound:
			added++
		default:
			return 0, err
		}
		if err := batch.Set(ek, p[1]); err != nil {
			return 0, err
		}
	}
	if batch.Empty() {
		return 0, nil
	}
	// Commit the new elements together with the updated header in one atomic batch
	// so a concurrent reader scanning the element keys never sees a torn set.
	header := storage.EncodeSparseHeader(config.TypeHash, int(curCount+added), 0, 0)
	if err := putValueBatch(batch, db, key, config.TypeHash, header, expireAt, expireAt); err != nil {
		return 0, err
	}
	if err := s.eng.Apply(batch); err != nil {
		return 0, err
	}
	s.dict.Set(db, key, config.TypeHash, expireAt, header)
	return added, nil
}

func (s *Store) hashGet(db uint16, key, field string) ([]byte, bool, error) {
	a, err := s.loadAgg(db, key, config.TypeHash)
	if err != nil {
		return nil, false, err
	}
	if !a.exists {
		return nil, false, nil
	}
	v, ok := a.hash[field]
	return v, ok, nil
}

func (s *Store) hashDel(db uint16, key string, fields []string) (int64, error) {
	a, err := s.loadAgg(db, key, config.TypeHash)
	if err != nil {
		return 0, err
	}
	if !a.exists {
		return 0, nil
	}
	a.ensureHash()
	var n int64
	for _, f := range fields {
		if _, ok := a.hash[f]; ok {
			delete(a.hash, f)
			n++
		}
	}
	if len(a.hash) == 0 {
		if _, err := s.deleteKey(db, key); err != nil {
			return 0, err
		}
		return n, nil
	}
	return n, s.saveAgg(db, key, a)
}

func (s *Store) hashIncrBy(db uint16, key, field string, delta int64) (int64, error) {
	a, err := s.loadAgg(db, key, config.TypeHash)
	if err != nil {
		return 0, err
	}
	a.ensureHash()
	var cur int64
	if v, ok := a.hash[field]; ok && len(v) > 0 {
		if cur, err = toInt64(v); err != nil {
			return 0, err
		}
	}
	if (delta > 0 && cur > maxInt64-delta) || (delta < 0 && cur < minInt64-delta) {
		return 0, ErrOverflow
	}
	next := cur + delta
	a.hash[field] = []byte(strconv.FormatInt(next, 10))
	if err := s.saveAgg(db, key, a); err != nil {
		return 0, err
	}
	return next, nil
}

func cmdHSet(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -3); err != nil {
		return err
	}
	if (len(args)-1)%2 != 0 {
		return WrongArgs("hset")
	}
	pairs := parsePairs(args[1:])
	n, err := c.Store.hashSet(c.DB, string(args[0]), pairs, false)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdHSetNX(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	pairs := [][2][]byte{{args[1], args[2]}}
	n, err := c.Store.hashSet(c.DB, string(args[0]), pairs, true)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdHGet(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	v, ok, err := c.Store.hashGet(c.DB, string(args[0]), string(args[1]))
	if err != nil {
		return err
	}
	if !ok {
		c.writeNull()
		return nil
	}
	c.writeBulk(v)
	return nil
}

func cmdHDel(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	fields := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		fields = append(fields, string(a))
	}
	n, err := c.Store.hashDel(c.DB, string(args[0]), fields)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdHExists(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	_, ok, err := c.Store.hashGet(c.DB, string(args[0]), string(args[1]))
	if err != nil {
		return err
	}
	c.writeInt(btoi(ok))
	return nil
}

func cmdHLen(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeHash)
	if err != nil {
		return err
	}
	c.writeInt(int64(len(a.hash)))
	return nil
}

func cmdHKeys(c *Ctx, args [][]byte) error {
	return hashEnumerate(c, args, true)
}

func cmdHVals(c *Ctx, args [][]byte) error {
	return hashEnumerate(c, args, false)
}

func hashEnumerate(c *Ctx, args [][]byte, keys bool) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeHash)
	if err != nil {
		return err
	}
	fields := sortedKeys(a.hash)
	c.w.WriteArray(len(fields))
	for _, f := range fields {
		if keys {
			c.w.WriteBulkString(f)
		} else {
			c.w.WriteBulk(a.hash[f])
		}
	}
	return nil
}

func cmdHGetAll(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeHash)
	if err != nil {
		return err
	}
	fields := sortedKeys(a.hash)
	c.w.WriteArray(len(fields) * 2)
	for _, f := range fields {
		c.w.WriteBulkString(f)
		c.w.WriteBulk(a.hash[f])
	}
	return nil
}

func cmdHMGet(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeHash)
	if err != nil {
		return err
	}
	c.w.WriteArray(len(args) - 1)
	for _, f := range args[1:] {
		v, ok := a.hash[string(f)]
		if !ok {
			c.writeNull()
			continue
		}
		c.writeBulk(v)
	}
	return nil
}

func cmdHMSet(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -3); err != nil {
		return err
	}
	if (len(args)-1)%2 != 0 {
		return WrongArgs("hmset")
	}
	if _, err := c.Store.hashSet(c.DB, string(args[0]), parsePairs(args[1:]), false); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

func cmdHIncrBy(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	d, err := toInt64(args[2])
	if err != nil {
		return err
	}
	n, err := c.Store.hashIncrBy(c.DB, string(args[0]), string(args[1]), d)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdHStrLen(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	v, ok, err := c.Store.hashGet(c.DB, string(args[0]), string(args[1]))
	if err != nil {
		return err
	}
	if !ok {
		c.writeInt(0)
		return nil
	}
	c.writeInt(int64(len(v)))
	return nil
}

func cmdHRandField(c *Ctx, args [][]byte) error {
	if len(args) < 1 || len(args) > 3 {
		return WrongArgs("hrandfield")
	}
	count := 1
	hasCount := false
	withValues := false
	if len(args) >= 2 {
		n, err := toInt64(args[1])
		if err != nil {
			return err
		}
		count = int(n)
		hasCount = true
	}
	// WithValues only appears after a count, so there is nothing to scan for
	// single-argument HRANDFIELD — and args[2:] would panic on a 1-arg call.
	if len(args) > 2 {
		for _, a := range args[2:] {
			if eqFold(a, "WITHVALUES") {
				withValues = true
			}
		}
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeHash)
	if err != nil {
		return err
	}
	fields := sortedKeys(a.hash)
	if len(fields) == 0 {
		// Redis: the no-count form replies with a single null bulk; any form
		// that passed a COUNT replies with an empty array.
		if !hasCount {
			c.writeNull()
		} else {
			c.w.WriteArray(0)
		}
		return nil
	}

	allowDup := count < 0
	n := count
	if n < 0 {
		n = -n
		if n < 0 {
			// -MinInt64 overflows; Redis rejects the overflow count.
			return &protoError{"ERR value is out of range"}
		}
	}
	if allowDup && n > 1<<30 {
		return &protoError{"ERR value is out of range"}
	}
	if n > len(fields) && !allowDup {
		n = len(fields)
	}
	pick := make([]string, 0, n)
	if allowDup {
		for range n {
			pick = append(pick, fields[rand.IntN(len(fields))])
		}
	} else {
		idx := rand.Perm(len(fields))[:n]
		for _, i := range idx {
			pick = append(pick, fields[i])
		}
	}

	// Redis: without a count argument the reply is a single field; once the
	// caller passes a count (even 1) the reply is always an array.
	if !hasCount {
		c.w.WriteBulkString(pick[0])
		return nil
	}
	if withValues {
		c.w.WriteArray(len(pick) * 2)
		for _, f := range pick {
			c.w.WriteBulkString(f)
			c.w.WriteBulk(a.hash[f])
		}
		return nil
	}
	c.w.WriteArray(len(pick))
	for _, f := range pick {
		c.w.WriteBulkString(f)
	}
	return nil
}

// parsePairs turns a flat field/value argument list into pairs.
func parsePairs(args [][]byte) [][2][]byte {
	out := make([][2][]byte, 0, len(args)/2)
	for i := 0; i+1 < len(args); i += 2 {
		out = append(out, [2][]byte{args[i], args[i+1]})
	}
	return out
}

func eqFold(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := range b {
		cb := b[i]
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		cs := s[i]
		if cs >= 'A' && cs <= 'Z' {
			cs += 'a' - 'A'
		}
		if cb != cs {
			return false
		}
	}
	return true
}

var _ = sort.Strings

// ---------- hash: HSCAN / HINCRBYFLOAT ----------

// cmdHScan implements HSCAN. The aggregate is fully materialised in memory and
// sorted, then paginated by COUNT like real Redis (COUNT is a hint for how many
// field-value pairs to return per call). The cursor is a decimal array-element
// offset; an unknown or out-of-range cursor restarts from the beginning, which
// matches SCAN's tolerant semantics.
func cmdHScan(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	// A sparse hash is stored as one key per field. saveAgg / hashSetSparse commit
	// the elements and the header in a single atomic batch (see aggregate.go), so a
	// concurrent HSET never leaves the collection half-written: this scan observes
	// either the old or the new state, never a torn in-between one.
	start, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return &protoError{"ERR invalid cursor"}
	}
	var matchFn func(string) bool
	noValues := false
	count := 10
	rest := args[2:]
	for len(rest) > 0 {
		switch strings.ToUpper(string(rest[0])) {
		case "MATCH":
			if len(rest) < 2 {
				return ErrSyntax
			}
			g, cerr := glob.Compile(string(rest[1]))
			if cerr != nil {
				return ErrSyntax
			}
			matchFn = g.Match
			rest = rest[2:]
		case "COUNT":
			if len(rest) < 2 {
				return ErrSyntax
			}
			cnt, perr := strconv.Atoi(string(rest[1]))
			if perr != nil {
				return ErrNotInteger
			}
			if cnt <= 0 {
				return ErrSyntax
			}
			count = cnt
			rest = rest[2:]
		case "NOVALUES":
			noValues = true
			rest = rest[1:]
		default:
			return ErrSyntax
		}
	}
	if matchFn == nil {
		matchFn = func(string) bool { return true }
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeHash)
	if err != nil {
		return err
	}
	flat := []string{}
	for _, f := range sortedKeys(a.hash) {
		if !matchFn(f) {
			continue
		}
		flat = append(flat, f)
		if !noValues {
			flat = append(flat, string(a.hash[f]))
		}
	}
	// COUNT counts field-value pairs, i.e. 2 array elements each.
	scanPageReply(c.w, flat, start, count*2)
	return nil
}

func cmdHIncrByFloat(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	inc, err := bigParseFloat(string(args[2]))
	if err != nil {
		return ErrNotFloat
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeHash)
	if err != nil {
		return err
	}
	curF := newBigPrec()
	if v, ok := a.hash[string(args[1])]; ok {
		if curF, err = bigParseFloat(string(v)); err != nil {
			return ErrNotFloat
		}
	}
	next := newBigPrec().Add(curF, inc)
	if next.IsInf() {
		return &protoError{"ERR increment would produce NaN or Infinity"}
	}
	field := string(args[1])
	if _, err := c.Store.hashSet(c.DB, string(args[0]),
		[][2][]byte{{[]byte(field), []byte(formatPrecFloat(next))}}, false); err != nil {
		return err
	}
	c.w.WriteBulkString(formatPrecFloat(next))
	return nil
}
