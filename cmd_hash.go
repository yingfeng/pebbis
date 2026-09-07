package redistore

import (
	"math/rand/v2"
	"sort"
	"strconv"

	"github.com/redistore/redistore/config"
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
	if err := s.eng.Apply(batch); err != nil {
		return 0, err
	}
	if added > 0 {
		if err := s.writeAggCount(db, key, config.TypeHash, int(curCount+added), 0, 0, expireAt, expireAt); err != nil {
			return 0, err
		}
	}
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
	withValues := false
	if len(args) >= 2 {
		n, err := toInt64(args[1])
		if err != nil {
			return err
		}
		count = int(n)
	}
	for _, a := range args[2:] {
		if eqFold(a, "WITHVALUES") {
			withValues = true
		}
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeHash)
	if err != nil {
		return err
	}
	fields := sortedKeys(a.hash)
	if len(fields) == 0 {
		if count == 1 && !withValues {
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

	if count == 1 && !withValues {
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
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

var _ = sort.Strings
