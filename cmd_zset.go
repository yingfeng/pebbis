package redistore

import (
	"sort"
	"strconv"
	"strings"

	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/storage"
)

// Sorted set commands.

// ZAddOption carries the flags of ZADD, mirroring cybergarage/go-redis.
type ZAddOption struct {
	XX, NX, LT, GT, CH, INCR bool
}

func (a *agg) ensureZSet() {
	if a.zset == nil {
		a.zset = map[string]float64{}
	}
}

// sortedMembers returns the members ordered by score then by member name.
func sortedMembers(m map[string]float64, desc bool) []storage.Member {
	out := make([]storage.Member, 0, len(m))
	for k, v := range m {
		out = append(out, storage.Member{Member: k, Score: v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			if desc {
				return out[i].Score > out[j].Score
			}
			return out[i].Score < out[j].Score
		}
		if desc {
			return out[i].Member > out[j].Member
		}
		return out[i].Member < out[j].Member
	})
	return out
}

// zAdd adds or updates members. It returns the number of new members, or the
// number changed when opt.CH is set. With opt.INCR it instead returns the new
// score of the single member as a float string.
func (s *Store) zAdd(db uint16, key string, members []storage.Member, opt ZAddOption) (added int64, incr *float64, err error) {
	// Sparse path: touch only the given members. The score index key has to be
	// rewritten whenever a score moves, which is why the old score is read
	// here rather than assumed.
	if obj, expireAt, exists, err := s.aggEncoding(db, key, config.TypeZSet); err != nil {
		return 0, nil, err
	} else if exists && obj.Enc == storage.EncSparse && !opt.INCR {
		return s.zAddSparse(db, key, members, opt, int64(obj.Count), expireAt)
	}

	a, err := s.loadAgg(db, key, config.TypeZSet)
	if err != nil {
		return 0, nil, err
	}
	a.ensureZSet()

	var changed int64
	for _, m := range members {
		old, exists := a.zset[m.Member]
		if opt.NX && exists {
			continue
		}
		if opt.XX && !exists {
			continue
		}
		newScore := m.Score
		if opt.INCR {
			if exists {
				newScore = old + m.Score
			}
		}
		if exists {
			if opt.GT && newScore <= old {
				continue
			}
			if opt.LT && newScore >= old {
				continue
			}
		}
		a.zset[m.Member] = newScore
		if !exists {
			added++
			changed++
		} else if old != newScore {
			changed++
		}
		if opt.INCR {
			v := newScore
			if err := s.saveAgg(db, key, a); err != nil {
				return 0, nil, err
			}
			return 0, &v, nil
		}
	}
	if err := s.saveAgg(db, key, a); err != nil {
		return 0, nil, err
	}
	if opt.CH {
		return changed, nil, nil
	}
	return added, nil, nil
}

// zAddSparse adds or updates members of a sparse sorted set in place.
func (s *Store) zAddSparse(db uint16, key string, members []storage.Member, opt ZAddOption, curCount, expireAt int64) (int64, *float64, error) {
	seg, _ := storage.SegmentFor(config.TypeZSet)
	batch := s.eng.Batch()
	defer batch.Close()

	var added, changed int64
	for _, m := range members {
		ek := storage.EncodeElemKey(seg, db, key, m.Member)
		v, closer, err := s.eng.Get(ek)
		exists := err == nil
		var old float64
		if exists {
			old = decodeScoreBytes(v)
			closer()
		} else if err != storage.ErrNotFound {
			return 0, nil, err
		}

		if opt.NX && exists {
			continue
		}
		if opt.XX && !exists {
			continue
		}
		newScore := m.Score
		if exists {
			if opt.GT && newScore <= old {
				continue
			}
			if opt.LT && newScore >= old {
				continue
			}
		}
		if err := batch.Set(ek, scoreBytes(newScore)); err != nil {
			return 0, nil, err
		}
		if exists && old != newScore {
			if err := batch.Delete(storage.EncodeScoreKey(db, key, old, m.Member)); err != nil {
				return 0, nil, err
			}
			changed++
		} else if !exists {
			added++
			changed++
		}
		if err := batch.Set(storage.EncodeScoreKey(db, key, newScore, m.Member), nil); err != nil {
			return 0, nil, err
		}
	}
	if batch.Empty() {
		return 0, nil, nil
	}
	if err := s.eng.Apply(batch); err != nil {
		return 0, nil, err
	}
	if added > 0 {
		if err := s.writeAggCount(db, key, config.TypeZSet, int(curCount+added), 0, 0, expireAt, expireAt); err != nil {
			return 0, nil, err
		}
	}
	if opt.CH {
		return changed, nil, nil
	}
	return added, nil, nil
}

// decodeScoreBytes reverses scoreBytes.
func decodeScoreBytes(b []byte) float64 {
	if len(b) < 8 {
		return 0
	}
	var u uint64
	for i := range 8 {
		u = u<<8 | uint64(b[i])
	}
	return storage.DecodeScore(u)
}

func (s *Store) zRem(db uint16, key string, members []string) (int64, error) {
	a, err := s.loadAgg(db, key, config.TypeZSet)
	if err != nil {
		return 0, err
	}
	if !a.exists {
		return 0, nil
	}
	a.ensureZSet()
	var n int64
	for _, m := range members {
		if _, ok := a.zset[m]; ok {
			delete(a.zset, m)
			n++
		}
	}
	if n == 0 {
		return 0, nil
	}
	if len(a.zset) == 0 {
		if _, err := s.deleteKey(db, key); err != nil {
			return 0, err
		}
		return n, nil
	}
	return n, s.saveAgg(db, key, a)
}

func (s *Store) zScore(db uint16, key, member string) (float64, bool, error) {
	a, err := s.loadAgg(db, key, config.TypeZSet)
	if err != nil {
		return 0, false, err
	}
	v, ok := a.zset[member]
	return v, ok, nil
}

func (s *Store) zRank(db uint16, key, member string, desc bool) (int64, bool, error) {
	a, err := s.loadAgg(db, key, config.TypeZSet)
	if err != nil {
		return 0, false, err
	}
	if _, ok := a.zset[member]; !ok {
		return 0, false, nil
	}
	for i, m := range sortedMembers(a.zset, desc) {
		if m.Member == member {
			return int64(i), true, nil
		}
	}
	return 0, false, nil
}

// zIncrBy adds delta to a member's score and returns the new score.
func (s *Store) zIncrBy(db uint16, key string, delta float64, member string) (float64, error) {
	a, err := s.loadAgg(db, key, config.TypeZSet)
	if err != nil {
		return 0, err
	}
	a.ensureZSet()
	next := a.zset[member] + delta
	a.zset[member] = next
	if err := s.saveAgg(db, key, a); err != nil {
		return 0, err
	}
	return next, nil
}

// zPopMinMax removes and returns the n lowest (or highest) scoring members.
func (s *Store) zPopMinMax(db uint16, key string, n int, max bool) ([]storage.Member, error) {
	a, err := s.loadAgg(db, key, config.TypeZSet)
	if err != nil {
		return nil, err
	}
	if len(a.zset) == 0 {
		return nil, nil
	}
	all := sortedMembers(a.zset, max)
	if n > len(all) {
		n = len(all)
	}
	out := all[:n]
	names := make([]string, 0, n)
	for _, m := range out {
		names = append(names, m.Member)
	}
	if _, err := s.zRem(db, key, names); err != nil {
		return nil, err
	}
	return out, nil
}

func cmdZAdd(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -3); err != nil {
		return err
	}
	opt := ZAddOption{}
	i := 1
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			opt.NX = true
		case "XX":
			opt.XX = true
		case "GT":
			opt.GT = true
		case "LT":
			opt.LT = true
		case "CH":
			opt.CH = true
		case "INCR":
			opt.INCR = true
		default:
			goto parse
		}
	}
parse:
	// Redis: NX vs XX are mutually exclusive, and GT/LT cannot combine with
	// NX. Must live after the parse label: the option scan's goto lands here.
	if opt.NX && (opt.XX || opt.GT || opt.LT) {
		return ErrSyntax
	}
	if opt.GT && opt.LT {
		return ErrSyntax
	}
	members := []storage.Member{}
	for ; i+1 < len(args); i += 2 {
		score, err := toFloat(args[i])
		if err != nil {
			return err
		}
		members = append(members, storage.Member{Score: score, Member: string(args[i+1])})
	}
	if i < len(args) {
		// Trailing token without a paired score: Redis rejects this outright.
		return ErrSyntax
	}
	if len(members) == 0 {
		return WrongArgs("zadd")
	}
	if opt.INCR && len(members) != 1 {
		return &protoError{"ERR INCR option supports a single increment-element pair"}
	}

	added, incr, err := c.Store.zAdd(c.DB, string(args[0]), members, opt)
	if err != nil {
		return err
	}
	// A new element may serve a blocked BZPOPMIN/BZPOPMAX.
	c.Store.notifyListChanged(c.DB, string(args[0]))
	if incr != nil {
		c.w.WriteBulkString(formatFloat(*incr))
		return nil
	}
	c.writeInt(added)
	return nil
}

func cmdZRem(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	n, err := c.Store.zRem(c.DB, string(args[0]), byteSliceToStrings(args[1:]))
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdZScore(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	v, ok, err := c.Store.zScore(c.DB, string(args[0]), string(args[1]))
	if err != nil {
		return err
	}
	if !ok {
		c.writeNull()
		return nil
	}
	c.w.WriteBulkString(formatFloat(v))
	return nil
}

func cmdZMScore(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeZSet)
	if err != nil {
		return err
	}
	c.w.WriteArray(len(args) - 1)
	for _, m := range args[1:] {
		v, ok := a.zset[string(m)]
		if !ok {
			c.writeNull()
			continue
		}
		c.w.WriteBulkString(formatFloat(v))
	}
	return nil
}

func cmdZCard(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeZSet)
	if err != nil {
		return err
	}
	c.writeInt(int64(len(a.zset)))
	return nil
}

func cmdZRank(c *Ctx, args [][]byte) error    { return rankCmd(c, args, false) }
func cmdZRevRank(c *Ctx, args [][]byte) error { return rankCmd(c, args, true) }

func rankCmd(c *Ctx, args [][]byte, desc bool) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	idx, ok, err := c.Store.zRank(c.DB, string(args[0]), string(args[1]), desc)
	if err != nil {
		return err
	}
	if !ok {
		c.writeNull()
		return nil
	}
	c.writeInt(idx)
	return nil
}

func cmdZIncrBy(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	d, err := toFloat(args[1])
	if err != nil {
		return err
	}
	v, err := c.Store.zIncrBy(c.DB, string(args[0]), d, string(args[2]))
	if err != nil {
		return err
	}
	c.w.WriteBulkString(formatFloat(v))
	return nil
}

// zRangeOpts captures the common ZRANGE / ZRANGEBYSCORE options.
type zRangeOpts struct {
	byScore   bool
	byLex     bool
	rev       bool
	withScore bool
	minExcl   bool
	maxExcl   bool
	offset    int
	count     int
	hasLimit  bool
}

func parseZRangeOpts(args [][]byte, i int) (zRangeOpts, error) {
	var o zRangeOpts
	o.count = -1
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "BYSCORE":
			o.byScore = true
		case "BYLEX":
			o.byLex = true
		case "REV":
			o.rev = true
		case "WITHSCORES":
			o.withScore = true
		case "LIMIT":
			if i+2 >= len(args) {
				return o, ErrSyntax
			}
			off, err := toInt64(args[i+1])
			if err != nil {
				return o, err
			}
			cnt, err := toInt64(args[i+2])
			if err != nil {
				return o, err
			}
			o.offset, o.count, o.hasLimit = int(off), int(cnt), true
			i += 2
		default:
			return o, ErrSyntax
		}
	}
	return o, nil
}

// parseScoreBound reads a score bound, handling the +/-inf and exclusive forms.
func parseScoreBound(b []byte) (float64, bool, error) {
	s := string(b)
	excl := false
	if strings.HasPrefix(s, "(") {
		excl = true
		s = s[1:]
	}
	switch strings.ToLower(s) {
	case "+inf":
		return infPos, excl, nil
	case "-inf":
		return infNeg, excl, nil
	}
	v, err := strconv.ParseFloat(s, 64)
	return v, excl, err
}

const (
	infPos = 1e308
	infNeg = -1e308
)

// cmdZRevRange is the legacy ZREVRANGE form: ZRANGE key start stop REV.
// go-redis still issues it for ZRevRange.
func cmdZRevRange(c *Ctx, args [][]byte) error {
	return cmdZRange(c, append(append([][]byte{}, args...), []byte("REV")))
}

func cmdZRange(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -3); err != nil {
		return err
	}
	start, err := toInt64(args[1])
	if err != nil {
		return err
	}
	stop, err := toInt64(args[2])
	if err != nil {
		return err
	}
	o, err := parseZRangeOpts(args, 3)
	if err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeZSet)
	if err != nil {
		return err
	}
	var items []storage.Member
	if o.byScore {
		items = filterByScore(sortedMembers(a.zset, false), start, stop, o.minExcl, o.maxExcl)
		if o.rev {
			reverseMembers(items)
		}
	} else {
		items = sortedMembers(a.zset, o.rev)
		items = indexRange(items, int(start), int(stop), o.rev)
	}
	if o.hasLimit {
		items = applyLimit(items, o.offset, o.count)
	}
	writeMembers(c, items, o.withScore)
	return nil
}

func cmdZRangeByScore(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -3); err != nil {
		return err
	}
	min, minEx, err := parseScoreBound(args[1])
	if err != nil {
		return ErrNotFloat
	}
	max, maxEx, err := parseScoreBound(args[2])
	if err != nil {
		return ErrNotFloat
	}
	o, err := parseZRangeOpts(args, 3)
	if err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeZSet)
	if err != nil {
		return err
	}
	items := filterByScoreRange(sortedMembers(a.zset, false), min, max, minEx, maxEx)
	if o.rev {
		reverseMembers(items)
	}
	if o.hasLimit {
		items = applyLimit(items, o.offset, o.count)
	}
	writeMembers(c, items, o.withScore)
	return nil
}

func cmdZCount(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	min, minEx, err := parseScoreBound(args[1])
	if err != nil {
		return ErrNotFloat
	}
	max, maxEx, err := parseScoreBound(args[2])
	if err != nil {
		return ErrNotFloat
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeZSet)
	if err != nil {
		return err
	}
	c.writeInt(int64(len(filterByScoreRange(sortedMembers(a.zset, false), min, max, minEx, maxEx))))
	return nil
}

func cmdZPopMin(c *Ctx, args [][]byte) error { return zPopCmd(c, args, false) }
func cmdZPopMax(c *Ctx, args [][]byte) error { return zPopCmd(c, args, true) }

func zPopCmd(c *Ctx, args [][]byte, max bool) error {
	if len(args) < 1 || len(args) > 2 {
		return WrongArgs("zpopmin")
	}
	n := 1
	if len(args) == 2 {
		v, err := toInt64(args[1])
		if err != nil {
			return err
		}
		if v < 0 {
			return &protoError{"ERR value is out of range, must be positive"}
		}
		n = int(v)
	}
	out, err := c.Store.zPopMinMax(c.DB, string(args[0]), n, max)
	if err != nil {
		return err
	}
	writeMembers(c, out, true)
	return nil
}

func cmdZRemRangeByRank(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	start, err := toInt64(args[1])
	if err != nil {
		return err
	}
	stop, err := toInt64(args[2])
	if err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeZSet)
	if err != nil {
		return err
	}
	if len(a.zset) == 0 {
		c.writeInt(0)
		return nil
	}
	all := sortedMembers(a.zset, false)
	victims := indexRange(all, int(start), int(stop), false)
	if len(victims) == 0 {
		c.writeInt(0)
		return nil
	}
	names := make([]string, 0, len(victims))
	for _, m := range victims {
		names = append(names, m.Member)
	}
	n, err := c.Store.zRem(c.DB, string(args[0]), names)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdZRemRangeByScore(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	min, minEx, err := parseScoreBound(args[1])
	if err != nil {
		return ErrNotFloat
	}
	max, maxEx, err := parseScoreBound(args[2])
	if err != nil {
		return ErrNotFloat
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeZSet)
	if err != nil {
		return err
	}
	victims := filterByScoreRange(sortedMembers(a.zset, false), min, max, minEx, maxEx)
	if len(victims) == 0 {
		c.writeInt(0)
		return nil
	}
	names := make([]string, 0, len(victims))
	for _, m := range victims {
		names = append(names, m.Member)
	}
	n, err := c.Store.zRem(c.DB, string(args[0]), names)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func writeMembers(c *Ctx, items []storage.Member, withScores bool) {
	n := len(items)
	if withScores {
		n *= 2
	}
	c.w.WriteArray(n)
	for _, m := range items {
		c.w.WriteBulkString(m.Member)
		if withScores {
			c.w.WriteBulkString(formatFloat(m.Score))
		}
	}
}

// indexRange applies Redis' inclusive, negative-aware index pair.
func indexRange(items []storage.Member, start, stop int, rev bool) []storage.Member {
	n := len(items)
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n || n == 0 {
		return nil
	}
	out := make([]storage.Member, stop-start+1)
	copy(out, items[start:stop+1])
	return out
}

// filterByScore keeps members whose rank falls in [start,stop]; used when
// ZRANGE is given BYSCORE with integer bounds.
func filterByScore(items []storage.Member, start, stop int64, minEx, maxEx bool) []storage.Member {
	return filterByScoreRange(items, float64(start), float64(stop), minEx, maxEx)
}

func filterByScoreRange(items []storage.Member, min, max float64, minEx, maxEx bool) []storage.Member {
	out := items[:0:0]
	for _, m := range items {
		if minEx {
			if m.Score <= min {
				continue
			}
		} else if m.Score < min {
			continue
		}
		if maxEx {
			if m.Score >= max {
				continue
			}
		} else if m.Score > max {
			continue
		}
		out = append(out, m)
	}
	return out
}

func reverseMembers(items []storage.Member) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

func applyLimit(items []storage.Member, offset, count int) []storage.Member {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return nil
	}
	items = items[offset:]
	if count < 0 {
		return items
	}
	if count > len(items) {
		count = len(items)
	}
	return items[:count]
}

// formatFloat renders a score the way Redis does.
func formatFloat(f float64) string {
	if f == float64(int64(f)) && f >= -1e17 && f <= 1e17 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', 17, 64)
}
