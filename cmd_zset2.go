package redistore

import (
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/storage"
)

// Sorted-set completion: lexicographic ranges, set operations, ZRANGESTORE,
// ZRANDMEMBER and the pop family. The score index and the member index are the
// same map here, so the lex family sorts in memory rather than scanning Pebble;
// the sparse member segment is already ordered, which the range scans use.

// ---------- lexicographic ordering ----------

// sortedByLex orders members lexicographically.
func sortedByLex(m map[string]float64, desc bool) []storage.Member {
	out := make([]storage.Member, 0, len(m))
	for k, v := range m {
		out = append(out, storage.Member{Member: k, Score: v})
	}
	sort.Slice(out, func(i, j int) bool {
		if desc {
			return out[i].Member > out[j].Member
		}
		return out[i].Member < out[j].Member
	})
	return out
}

// lexBound is one endpoint of a lex range: [x inclusive, (x exclusive, or the
// open-ended - / +.
type lexBound struct {
	value string
	excl  bool
	inf   bool
}

func parseLexBound(b []byte) (lexBound, error) {
	s := string(b)
	switch s {
	case "-":
		return lexBound{inf: true}, nil
	case "+":
		return lexBound{inf: true}, nil
	}
	var lb lexBound
	switch {
	case strings.HasPrefix(s, "["):
		s = s[1:]
	case strings.HasPrefix(s, "("):
		lb.excl = true
		s = s[1:]
	default:
		return lb, ErrSyntax
	}
	if s == "" {
		return lb, ErrSyntax
	}
	lb.value = s
	return lb, nil
}

func lexInRange(items []storage.Member, min, max lexBound) []storage.Member {
	out := items[:0:0]
	for _, m := range items {
		if !min.inf {
			if min.excl {
				if m.Member <= min.value {
					continue
				}
			} else if m.Member < min.value {
				continue
			}
		}
		if !max.inf {
			if max.excl {
				if m.Member >= max.value {
					continue
				}
			} else if m.Member > max.value {
				continue
			}
		}
		out = append(out, m)
	}
	return out
}

func cmdZLexCount(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	min, err := parseLexBound(args[1])
	if err != nil {
		return err
	}
	max, err := parseLexBound(args[2])
	if err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeZSet)
	if err != nil {
		return err
	}
	c.writeInt(int64(len(lexInRange(sortedByLex(a.zset, false), min, max))))
	return nil
}

func cmdZRangeByLex(c *Ctx, args [][]byte) error    { return zRangeLex(c, args, false) }
func cmdZRevRangeByLex(c *Ctx, args [][]byte) error { return zRangeLex(c, args, true) }

// zRangeLex implements ZRANGEBYLEX / ZREVRANGEBYLEX. The REV form takes
// (max, min) argument order, unlike the plain form.
func zRangeLex(c *Ctx, args [][]byte, rev bool) error {
	if len(args) < 3 {
		return WrongArgs("zrangebylex")
	}
	b1, err := parseLexBound(args[1])
	if err != nil {
		return err
	}
	b2, err := parseLexBound(args[2])
	if err != nil {
		return err
	}
	min, max := b1, b2
	if rev {
		min, max = b2, b1
	}
	o, err := parseZRangeOpts(args, 3)
	if err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeZSet)
	if err != nil {
		return err
	}
	items := lexInRange(sortedByLex(a.zset, false), min, max)
	if rev {
		reverseMembers(items)
	}
	if o.hasLimit {
		items = applyLimit(items, o.offset, o.count)
	}
	writeMembers(c, items, o.withScore)
	return nil
}

func cmdZRemRangeByLex(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	min, err := parseLexBound(args[1])
	if err != nil {
		return err
	}
	max, err := parseLexBound(args[2])
	if err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeZSet)
	if err != nil {
		return err
	}
	victims := lexInRange(sortedByLex(a.zset, false), min, max)
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

// ---------- set operations across sorted sets ----------

type zAggMode int

const (
	zAggSum zAggMode = iota
	zAggMin
	zAggMax
)

// zOpResult loads every source set and combines it into one result. ZDIFF
// ignores weights and aggregation, matching Redis.
func zOpResult(c *Ctx, keys []string, weights []float64, agg zAggMode, op int) (map[string]float64, error) {
	sets := make([]map[string]float64, 0, len(keys))
	for _, k := range keys {
		a, err := c.Store.loadAgg(c.DB, k, config.TypeZSet)
		if err != nil {
			return nil, err
		}
		sets = append(sets, a.zset)
	}
	out := map[string]float64{}
	switch op {
	case opUnion:
		for i, s := range sets {
			w := 1.0
			if i < len(weights) {
				w = weights[i]
			}
			for m, sc := range s {
				v := sc * w
				if old, ok := out[m]; ok {
					out[m] = zApplyAgg(old, v, agg)
				} else {
					out[m] = v
				}
			}
		}
	case opInter:
		if len(sets) == 0 {
			return out, nil
		}
		for m := range sets[0] {
			out[m] = 0
		}
		for i, s := range sets {
			w := 1.0
			if i < len(weights) {
				w = weights[i]
			}
			for m := range out {
				sc, ok := s[m]
				if !ok {
					delete(out, m)
					continue
				}
				v := sc * w
				if i == 0 {
					out[m] = v
				} else {
					out[m] = zApplyAgg(out[m], v, agg)
				}
			}
		}
	case opDiff:
		if len(sets) == 0 {
			return out, nil
		}
		for m := range sets[0] {
			blocked := false
			for _, s := range sets[1:] {
				if _, ok := s[m]; ok {
					blocked = true
					break
				}
			}
			if !blocked {
				out[m] = sets[0][m]
			}
		}
	}
	return out, nil
}

func zApplyAgg(a, b float64, agg zAggMode) float64 {
	switch agg {
	case zAggMin:
		if b < a {
			return b
		}
		return a
	case zAggMax:
		if b > a {
			return b
		}
		return a
	}
	return a + b
}

// zParseOpArgs splits "numkeys key... [WEIGHTS w...] [AGGREGATE m] [WITHSCORES]"
// into its parts. withScores is only accepted by the non-storing forms.
func zParseOpArgs(args [][]byte, allowWithScores bool) (keys []string, weights []float64, agg zAggMode, withScores bool, err error) {
	if len(args) < 1 {
		return nil, nil, 0, false, WrongArgs("zunion")
	}
	nk, err := toInt64(args[0])
	if err != nil || nk <= 0 || int(nk) > len(args)-1 {
		return nil, nil, 0, false, &protoError{"ERR numkeys should be greater than 0 and no larger than the number of keys"}
	}
	keys = byteSliceToStrings(args[1 : 1+int(nk)])
	agg = zAggSum
	i := 1 + int(nk)
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "WEIGHTS":
			if i+int(nk) >= len(args) {
				return nil, nil, 0, false, ErrSyntax
			}
			for j := 0; j < int(nk); j++ {
				w, perr := toFloat(args[i+1+j])
				if perr != nil {
					return nil, nil, 0, false, ErrNotFloat
				}
				weights = append(weights, w)
			}
			i += 1 + int(nk)
		case "AGGREGATE":
			if i+1 >= len(args) {
				return nil, nil, 0, false, ErrSyntax
			}
			switch strings.ToUpper(string(args[i+1])) {
			case "SUM":
				agg = zAggSum
			case "MIN":
				agg = zAggMin
			case "MAX":
				agg = zAggMax
			default:
				return nil, nil, 0, false, ErrSyntax
			}
			i += 2
		case "WITHSCORES":
			if !allowWithScores {
				return nil, nil, 0, false, ErrSyntax
			}
			withScores = true
			i++
		default:
			return nil, nil, 0, false, ErrSyntax
		}
	}
	return keys, weights, agg, withScores, nil
}

func cmdZUnion(c *Ctx, args [][]byte) error {
	keys, weights, agg, withScores, err := zParseOpArgs(args, true)
	if err != nil {
		return err
	}
	out, err := zOpResult(c, keys, weights, agg, opUnion)
	if err != nil {
		return err
	}
	writeMembers(c, sortedMembers(out, false), withScores)
	return nil
}

func cmdZInter(c *Ctx, args [][]byte) error {
	keys, weights, agg, withScores, err := zParseOpArgs(args, true)
	if err != nil {
		return err
	}
	out, err := zOpResult(c, keys, weights, agg, opInter)
	if err != nil {
		return err
	}
	writeMembers(c, sortedMembers(out, false), withScores)
	return nil
}

func cmdZDiff(c *Ctx, args [][]byte) error {
	if len(args) < 1 {
		return WrongArgs("zdiff")
	}
	nk, err := toInt64(args[0])
	if err != nil || nk <= 0 || int(nk) > len(args)-1 {
		return &protoError{"ERR numkeys should be greater than 0 and no larger than the number of keys"}
	}
	keys := byteSliceToStrings(args[1 : 1+int(nk)])
	withScores := false
	for _, a := range args[1+nk:] {
		if strings.EqualFold(string(a), "WITHSCORES") {
			withScores = true
		}
	}
	out, err := zOpResult(c, keys, nil, zAggSum, opDiff)
	if err != nil {
		return err
	}
	writeMembers(c, sortedMembers(out, false), withScores)
	return nil
}

// zStoreOp is the *STORE form: same combination, result lands in dst.
func zStoreOp(c *Ctx, args [][]byte, op int) error {
	if len(args) < 2 {
		return WrongArgs("zunionstore")
	}
	dst := string(args[0])
	keys, weights, aggMode, _, err := zParseOpArgs(args[1:], false)
	if err != nil {
		return err
	}
	out, err := zOpResult(c, keys, weights, aggMode, op)
	if err != nil {
		return err
	}
	if _, err := c.Store.deleteKey(c.DB, dst); err != nil {
		return err
	}
	if len(out) == 0 {
		c.writeInt(0)
		return nil
	}
	a := &agg{typ: config.TypeZSet, zset: out, exists: true}
	if err := c.Store.saveAgg(c.DB, dst, a); err != nil {
		return err
	}
	c.writeInt(int64(len(out)))
	return nil
}

func cmdZUnionStore(c *Ctx, args [][]byte) error { return zStoreOp(c, args, opUnion) }
func cmdZInterStore(c *Ctx, args [][]byte) error { return zStoreOp(c, args, opInter) }
func cmdZDiffStore(c *Ctx, args [][]byte) error  { return zStoreOp(c, args, opDiff) }

// ---------- ZRANGESTORE / ZRANDMEMBER ----------

func cmdZRangeStore(c *Ctx, args [][]byte) error {
	if len(args) < 4 {
		return WrongArgs("zrangestore")
	}
	dst, src := string(args[0]), string(args[1])
	o, err := parseZRangeOpts(args, 4)
	if err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, src, config.TypeZSet)
	if err != nil {
		return err
	}
	var items []storage.Member
	if o.byLex {
		b1, perr := parseLexBound(args[2])
		if perr != nil {
			return perr
		}
		b2, perr := parseLexBound(args[3])
		if perr != nil {
			return perr
		}
		min, max := b1, b2
		if o.rev {
			min, max = b2, b1
		}
		items = lexInRange(sortedByLex(a.zset, false), min, max)
	} else {
		start, perr := toInt64(args[2])
		if perr != nil {
			return perr
		}
		stop, perr := toInt64(args[3])
		if perr != nil {
			return perr
		}
		items = indexRange(sortedMembers(a.zset, o.rev), int(start), int(stop), o.rev)
	}
	if o.hasLimit {
		items = applyLimit(items, o.offset, o.count)
	}
	if _, err := c.Store.deleteKey(c.DB, dst); err != nil {
		return err
	}
	if len(items) == 0 {
		c.writeInt(0)
		return nil
	}
	na := &agg{typ: config.TypeZSet, zset: map[string]float64{}, exists: true}
	for _, m := range items {
		na.zset[m.Member] = m.Score
	}
	if err := c.Store.saveAgg(c.DB, dst, na); err != nil {
		return err
	}
	c.writeInt(int64(len(items)))
	return nil
}

func cmdZRandMember(c *Ctx, args [][]byte) error {
	if len(args) < 1 || len(args) > 3 {
		return WrongArgs("zrandmember")
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeZSet)
	if err != nil {
		return err
	}
	all := sortedMembers(a.zset, false)
	withScores := len(args) == 3 && strings.EqualFold(string(args[2]), "WITHSCORES")
	if len(args) == 1 {
		if len(all) == 0 {
			c.writeNull()
			return nil
		}
		c.w.WriteBulkString(all[rand.IntN(len(all))].Member)
		return nil
	}
	n, err := toInt64(args[1])
	if err != nil {
		return err
	}
	if len(all) == 0 {
		c.w.WriteArray(0)
		return nil
	}
	count := int(n)
	if count < 0 {
		// Negative: repeats allowed.
		count = -count
		out := make([]storage.Member, 0, count)
		for range count {
			out = append(out, all[rand.IntN(len(all))])
		}
		writeMembers(c, out, withScores)
		return nil
	}
	if count > len(all) {
		count = len(all)
	}
	idx := rand.Perm(len(all))[:count]
	out := make([]storage.Member, 0, count)
	for _, i := range idx {
		out = append(out, all[i])
	}
	writeMembers(c, out, withScores)
	return nil
}

// ---------- ZMPOP / BZPOPMIN / BZPOPMAX / BZMPOP ----------

// zPopFromKeys pops from the first non-empty zset, in the given direction.
func zPopFromKeys(c *Ctx, keys []string, count int, max bool) (string, []storage.Member, error) {
	for _, k := range keys {
		a, err := c.Store.loadAgg(c.DB, k, config.TypeZSet)
		if err != nil {
			return "", nil, err
		}
		if len(a.zset) == 0 {
			continue
		}
		out, err := c.Store.zPopMinMax(c.DB, k, count, max)
		if err != nil {
			return "", nil, err
		}
		return k, out, nil
	}
	return "", nil, nil
}

func writeZPopReply(c *Ctx, key string, items []storage.Member) {
	if key == "" {
		c.writeNull()
		return
	}
	c.w.WriteArray(2)
	c.w.WriteBulkString(key)
	writeMembers(c, items, true)
}

func cmdZMPop(c *Ctx, args [][]byte) error {
	if len(args) < 2 {
		return WrongArgs("zmpop")
	}
	nk, err := toInt64(args[0])
	if err != nil || nk <= 0 || int(nk) > len(args)-1 {
		return &protoError{"ERR numkeys should be greater than 0 and no larger than the number of keys"}
	}
	keys := byteSliceToStrings(args[1 : 1+int(nk)])
	max := false
	count := 1
	for i := int(nk) + 1; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "MIN":
			max = false
		case "MAX":
			max = true
		case "COUNT":
			if i+1 >= len(args) {
				return ErrSyntax
			}
			v, perr := toInt64(args[i+1])
			if perr != nil {
				return perr
			}
			count = int(v)
			i++
		}
	}
	key, items, err := zPopFromKeys(c, keys, count, max)
	if err != nil {
		return err
	}
	writeZPopReply(c, key, items)
	return nil
}

// cmdBZPopMin handles both BZPOPMIN and BZPOPMAX: "key... timeout".
func cmdBZPopMin(c *Ctx, args [][]byte) error { return bzPopCmd(c, args, false) }
func cmdBZPopMax(c *Ctx, args [][]byte) error { return bzPopCmd(c, args, true) }

func bzPopCmd(c *Ctx, args [][]byte, max bool) error {
	if len(args) < 2 {
		return WrongArgs("bzpopmin")
	}
	secs, err := toInt64(args[len(args)-1])
	if err != nil {
		return err
	}
	if secs < 0 {
		return &protoError{"ERR timeout is negative"}
	}
	keys := byteSliceToStrings(args[:len(args)-1])
	timeout := time.Duration(secs) * time.Second
	deadline := time.Time{}
	if secs > 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		key, items, err := zPopFromKeys(c, keys, 1, max)
		if err != nil {
			return err
		}
		if key != "" {
			c.w.WriteArray(3)
			c.w.WriteBulkString(key)
			c.w.WriteBulkString(items[0].Member)
			c.w.WriteBulkString(formatFloat(items[0].Score))
			return nil
		}
		if secs > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				c.writeNull()
				return nil
			}
			timeout = remaining
		}
		if !c.Store.blockWait(c.DB, keys, timeout) {
			c.writeNull()
			return nil
		}
	}
}

// cmdBZMPop is the blocking ZMPOP: "timeout numkeys key... MIN|MAX [COUNT n]".
func cmdBZMPop(c *Ctx, args [][]byte) error {
	if len(args) < 3 {
		return WrongArgs("bzmpop")
	}
	secs, err := toInt64(args[0])
	if err != nil {
		return err
	}
	if secs < 0 {
		return &protoError{"ERR timeout is negative"}
	}
	nk, err := toInt64(args[1])
	if err != nil || nk <= 0 || int(nk) > len(args)-2 {
		return &protoError{"ERR numkeys should be greater than 0 and no larger than the number of keys"}
	}
	keys := byteSliceToStrings(args[2 : 2+nk])
	rest := args[2+nk:]
	max := false
	count := 1
	for i := 0; i < len(rest); i++ {
		switch strings.ToUpper(string(rest[i])) {
		case "MIN":
			max = false
		case "MAX":
			max = true
		case "COUNT":
			if i+1 >= len(rest) {
				return ErrSyntax
			}
			v, perr := toInt64(rest[i+1])
			if perr != nil {
				return perr
			}
			count = int(v)
			i++
		}
	}
	timeout := time.Duration(secs) * time.Second
	deadline := time.Time{}
	if secs > 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		key, items, err := zPopFromKeys(c, keys, count, max)
		if err != nil {
			return err
		}
		if key != "" {
			writeZPopReply(c, key, items)
			return nil
		}
		if secs > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				c.writeNull()
				return nil
			}
			timeout = remaining
		}
		if !c.Store.blockWait(c.DB, keys, timeout) {
			c.writeNull()
			return nil
		}
	}
}
