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

// Set commands.

func (a *agg) ensureSet() {
	if a.set == nil {
		a.set = map[string]struct{}{}
	}
}

func (s *Store) setAdd(db uint16, key string, members []string) (int64, error) {
	if obj, expireAt, exists, err := s.aggEncoding(db, key, config.TypeSet); err != nil {
		return 0, err
	} else if exists && obj.Enc == storage.EncSparse {
		return s.setAddSparse(db, key, members, int64(obj.Count), expireAt)
	}

	a, err := s.loadAgg(db, key, config.TypeSet)
	if err != nil {
		return 0, err
	}
	a.ensureSet()
	var added int64
	for _, m := range members {
		if _, ok := a.set[m]; ok {
			continue
		}
		a.set[m] = struct{}{}
		added++
	}
	if added == 0 {
		return 0, nil
	}
	return added, s.saveAgg(db, key, a)
}

// setAddSparse adds members to a sparse set without scanning the rest.
func (s *Store) setAddSparse(db uint16, key string, members []string, curCount, expireAt int64) (int64, error) {
	// Per-key lock held by the caller (dispatch) for the whole command.
	seg, _ := storage.SegmentFor(config.TypeSet)
	batch := s.eng.Batch()
	defer batch.Close()

	var added int64
	for _, m := range members {
		ek := storage.EncodeElemKey(seg, db, key, m)
		_, closer, err := s.eng.Get(ek)
		if err == nil {
			closer()
			continue
		}
		if err != storage.ErrNotFound {
			return 0, err
		}
		if err := batch.Set(ek, nil); err != nil {
			return 0, err
		}
		added++
	}
	if added == 0 {
		return 0, nil
	}
	// Commit the new members together with the updated header in one atomic batch
	// so a concurrent reader scanning the member keys never sees a torn set.
	header := storage.EncodeSparseHeader(config.TypeSet, int(curCount+added), 0, 0)
	if err := putValueBatch(batch, db, key, config.TypeSet, header, expireAt, expireAt); err != nil {
		return 0, err
	}
	if err := s.eng.Apply(batch); err != nil {
		return 0, err
	}
	s.dict.Set(db, key, config.TypeSet, expireAt, header)
	return added, nil
}

func (s *Store) setRem(db uint16, key string, members []string) (int64, error) {
	a, err := s.loadAgg(db, key, config.TypeSet)
	if err != nil {
		return 0, err
	}
	if !a.exists {
		return 0, nil
	}
	a.ensureSet()
	var n int64
	for _, m := range members {
		if _, ok := a.set[m]; ok {
			delete(a.set, m)
			n++
		}
	}
	if n == 0 {
		return 0, nil
	}
	if len(a.set) == 0 {
		if _, err := s.deleteKey(db, key); err != nil {
			return 0, err
		}
		return n, nil
	}
	return n, s.saveAgg(db, key, a)
}

func (s *Store) setMembers(db uint16, key string) ([]string, error) {
	a, err := s.loadAgg(db, key, config.TypeSet)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(a.set))
	for m := range a.set {
		out = append(out, m)
	}
	sort.Strings(out)
	return out, nil
}

func (s *Store) setIsMember(db uint16, key, member string) (bool, error) {
	a, err := s.loadAgg(db, key, config.TypeSet)
	if err != nil {
		return false, err
	}
	_, ok := a.set[member]
	return ok, nil
}

func (s *Store) setIsMemberMulti(db uint16, key string, members []string) ([]bool, error) {
	a, err := s.loadAgg(db, key, config.TypeSet)
	if err != nil {
		return nil, err
	}
	out := make([]bool, len(members))
	for i, m := range members {
		_, out[i] = a.set[m]
	}
	return out, nil
}

// setMove transfers a member between two sets.
func (s *Store) setMove(db uint16, src, dst, member string) (bool, error) {
	a, err := s.loadAgg(db, src, config.TypeSet)
	if err != nil {
		return false, err
	}
	// Redis's smoveCommand looks up both keys and validates their types
	// *before* testing membership, so a wrong-type destination must error
	// even when the member is absent from the (possibly missing) source.
	if _, _, _, err := s.aggEncoding(db, dst, config.TypeSet); err != nil {
		return false, err
	}
	if _, ok := a.set[member]; !ok {
		return false, nil
	}
	if _, err := s.setRem(db, src, []string{member}); err != nil {
		return false, err
	}
	if _, err := s.setAdd(db, dst, []string{member}); err != nil {
		return false, err
	}
	return true, nil
}

// setPop removes and returns up to n random members.
func (s *Store) setPop(db uint16, key string, n int) ([]string, error) {
	a, err := s.loadAgg(db, key, config.TypeSet)
	if err != nil {
		return nil, err
	}
	if len(a.set) == 0 {
		return nil, nil
	}
	all := make([]string, 0, len(a.set))
	for m := range a.set {
		all = append(all, m)
	}
	sort.Strings(all)
	if n > len(all) {
		n = len(all)
	}
	out := make([]string, 0, n)
	for range n {
		i := rand.IntN(len(all))
		out = append(out, all[i])
		all[i] = all[len(all)-1]
		all = all[:len(all)-1]
	}
	if _, err := s.setRem(db, key, out); err != nil {
		return nil, err
	}
	return out, nil
}

func cmdSAdd(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	n, err := c.Store.setAdd(c.DB, string(args[0]), byteSliceToStrings(args[1:]))
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdSRem(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	n, err := c.Store.setRem(c.DB, string(args[0]), byteSliceToStrings(args[1:]))
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdSMembers(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	out, err := c.Store.setMembers(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	c.w.WriteArray(len(out))
	for _, m := range out {
		c.w.WriteBulkString(m)
	}
	return nil
}

func cmdSIsMember(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	ok, err := c.Store.setIsMember(c.DB, string(args[0]), string(args[1]))
	if err != nil {
		return err
	}
	c.writeInt(btoi(ok))
	return nil
}

func cmdSMIsMember(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	ok, err := c.Store.setIsMemberMulti(c.DB, string(args[0]), byteSliceToStrings(args[1:]))
	if err != nil {
		return err
	}
	c.w.WriteArray(len(ok))
	for _, b := range ok {
		c.writeInt(btoi(b))
	}
	return nil
}

func cmdSCard(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeSet)
	if err != nil {
		return err
	}
	c.writeInt(int64(len(a.set)))
	return nil
}

func cmdSMove(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	ok, err := c.Store.setMove(c.DB, string(args[0]), string(args[1]), string(args[2]))
	if err != nil {
		return err
	}
	c.writeInt(btoi(ok))
	return nil
}

func cmdSPop(c *Ctx, args [][]byte) error {
	if len(args) < 1 || len(args) > 2 {
		return WrongArgs("spop")
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
	out, err := c.Store.setPop(c.DB, string(args[0]), n)
	if err != nil {
		return err
	}
	if len(args) == 1 {
		if len(out) == 0 {
			c.writeNull()
		} else {
			c.w.WriteBulkString(out[0])
		}
		return nil
	}
	c.w.WriteArray(len(out))
	for _, m := range out {
		c.w.WriteBulkString(m)
	}
	return nil
}

func cmdSRandMember(c *Ctx, args [][]byte) error {
	if len(args) < 1 {
		return WrongArgs("srandmember")
	}
	if len(args) > 2 {
		// Redis checks the option list after the count and reports a syntax
		// error for trailing arguments.
		return ErrSyntax
	}
	members, err := c.Store.setMembers(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if len(members) == 0 {
		if len(args) == 1 {
			c.writeNull()
		} else {
			c.w.WriteArray(0)
		}
		return nil
	}
	if len(args) == 1 {
		c.w.WriteBulkString(members[rand.IntN(len(members))])
		return nil
	}
	n, err := toInt64(args[1])
	if err != nil {
		return err
	}
	if n < 0 {
		// Negative count allows repeats.
		k := -n
		if k < 0 {
			return &protoError{"ERR value is out of range"}
		}
		if k > 1<<30 {
			return &protoError{"ERR value is out of range"}
		}
		m := int(k)
		c.w.WriteArray(m)
		for range m {
			c.w.WriteBulkString(members[rand.IntN(len(members))])
		}
		return nil
	}
	if int(n) > len(members) {
		n = int64(len(members))
	}
	idx := rand.Perm(len(members))[:n]
	c.w.WriteArray(len(idx))
	for _, i := range idx {
		c.w.WriteBulkString(members[i])
	}
	return nil
}

// setOp evaluates a multi-key set expression.
func setOp(c *Ctx, args [][]byte, op int) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	keys := byteSliceToStrings(args)
	sets := make([]map[string]struct{}, 0, len(keys))
	for _, k := range keys {
		a, err := c.Store.loadAgg(c.DB, k, config.TypeSet)
		if err != nil {
			return err
		}
		sets = append(sets, a.set)
	}
	var out map[string]struct{}
	switch op {
	case opUnion:
		out = map[string]struct{}{}
		for _, m := range sets {
			for k := range m {
				out[k] = struct{}{}
			}
		}
	case opInter:
		out = map[string]struct{}{}
		if len(sets) > 0 {
			for k := range sets[0] {
				out[k] = struct{}{}
			}
			for _, s := range sets[1:] {
				for k := range out {
					if _, ok := s[k]; !ok {
						delete(out, k)
					}
				}
			}
		}
	case opDiff:
		out = map[string]struct{}{}
		if len(sets) > 0 {
			for k := range sets[0] {
				out[k] = struct{}{}
			}
			for _, s := range sets[1:] {
				for k := range out {
					if _, ok := s[k]; ok {
						delete(out, k)
					}
				}
			}
		}
	}
	res := make([]string, 0, len(out))
	for k := range out {
		res = append(res, k)
	}
	sort.Strings(res)
	c.w.WriteArray(len(res))
	for _, m := range res {
		c.w.WriteBulkString(m)
	}
	return nil
}

const (
	opUnion = iota
	opInter
	opDiff
)

func cmdSUnion(c *Ctx, args [][]byte) error { return setOp(c, args, opUnion) }
func cmdSInter(c *Ctx, args [][]byte) error { return setOp(c, args, opInter) }
func cmdSDiff(c *Ctx, args [][]byte) error  { return setOp(c, args, opDiff) }

// setStoreOp runs opUnion/opInter/opDiff and stores the result in args[0].
func setStoreOp(c *Ctx, args [][]byte, op int) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	dst := string(args[0])
	keys := byteSliceToStrings(args[1:])
	sets := make([]map[string]struct{}, 0, len(keys))
	for _, k := range keys {
		a, err := c.Store.loadAgg(c.DB, k, config.TypeSet)
		if err != nil {
			return err
		}
		sets = append(sets, a.set)
	}
	var out map[string]struct{}
	switch op {
	case opUnion:
		out = map[string]struct{}{}
		for _, m := range sets {
			for k := range m {
				out[k] = struct{}{}
			}
		}
	case opInter:
		out = map[string]struct{}{}
		if len(sets) > 0 {
			for k := range sets[0] {
				out[k] = struct{}{}
			}
			for _, s := range sets[1:] {
				for k := range out {
					if _, ok := s[k]; !ok {
						delete(out, k)
					}
				}
			}
		}
	case opDiff:
		out = map[string]struct{}{}
		if len(sets) > 0 {
			for k := range sets[0] {
				out[k] = struct{}{}
			}
			for _, s := range sets[1:] {
				for k := range out {
					if _, ok := s[k]; ok {
						delete(out, k)
					}
				}
			}
		}
	}
	if len(out) == 0 {
		if _, err := c.Store.deleteKey(c.DB, dst); err != nil {
			return err
		}
		c.writeInt(0)
		return nil
	}
	members := make([]string, 0, len(out))
	for k := range out {
		members = append(members, k)
	}
	// Replace rather than merge, matching Redis.
	if _, err := c.Store.deleteKey(c.DB, dst); err != nil {
		return err
	}
	n, err := c.Store.setAdd(c.DB, dst, members)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdSUnionStore(c *Ctx, args [][]byte) error { return setStoreOp(c, args, opUnion) }
func cmdSInterStore(c *Ctx, args [][]byte) error { return setStoreOp(c, args, opInter) }
func cmdSDiffStore(c *Ctx, args [][]byte) error  { return setStoreOp(c, args, opDiff) }

func cmdSInterCard(c *Ctx, args [][]byte) error {
	if len(args) < 2 {
		return WrongArgs("sintercard")
	}
	numkeys, err := toInt64(args[0])
	if err != nil {
		return &protoError{"ERR numkeys should be greater than 0"}
	}
	if numkeys <= 0 {
		return &protoError{"ERR numkeys should be greater than 0"}
	}
	if int(numkeys) > len(args)-1 {
		return &protoError{"ERR Number of keys can't be greater than number of args"}
	}
	keys := byteSliceToStrings(args[1 : 1+numkeys])
	// Optional trailing LIMIT <n>; nothing else may follow the keys.
	limit := 0
	if rest := args[1+numkeys:]; len(rest) > 0 {
		if len(rest) != 2 || !eqFold(rest[0], "limit") { // eqFold compares against a lower-case literal
			return ErrSyntax
		}
		v, err := toInt64(rest[1])
		if err != nil {
			return err
		}
		if v < 0 {
			return &protoError{"ERR LIMIT can't be negative"}
		}
		limit = int(v) // 0 means "no limit", matching Redis
	}
	if len(keys) == 0 {
		c.writeInt(0)
		return nil
	}
	// Load every key first: Redis type-checks all of them even when an
	// earlier key is missing (SINTERCARD 2 nosuch str is a WRONGTYPE).
	aggs := make([]*agg, 0, len(keys))
	for _, k := range keys {
		a, err := c.Store.loadAgg(c.DB, k, config.TypeSet)
		if err != nil {
			return err
		}
		aggs = append(aggs, a)
	}
	var inter map[string]struct{}
	for i, a := range aggs {
		if i == 0 {
			inter = map[string]struct{}{}
			for m := range a.set {
				inter[m] = struct{}{}
			}
			continue
		}
		for m := range inter {
			if _, ok := a.set[m]; !ok {
				delete(inter, m)
			}
		}
	}
	n := len(inter)
	if limit > 0 && n > limit {
		n = limit
	}
	c.writeInt(int64(n))
	return nil
}

// ---------- set: SSCAN ----------

// cmdSScan implements SSCAN. The member set is fully materialised, sorted and
// paginated by COUNT (COUNT counts members, one array element each). The cursor
// is a decimal array-element offset; an unknown or out-of-range cursor restarts
// from the beginning, which matches SCAN's tolerant semantics.
func cmdSScan(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	// A sparse set is stored as one key per member. saveAgg / setAddSparse commit
	// the members and the header in a single atomic batch, so a concurrent SADD
	// never leaves the collection half-written: this scan observes either the old or
	// the new state, never a torn in-between one.
	start, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return &protoError{"ERR invalid cursor"}
	}
	var matchFn func(string) bool
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
		default:
			return ErrSyntax
		}
	}
	if matchFn == nil {
		matchFn = func(string) bool { return true }
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeSet)
	if err != nil {
		return err
	}
	members := make([]string, 0, len(a.set))
	for m := range a.set {
		members = append(members, m)
	}
	sort.Strings(members)
	out := make([]string, 0, len(members))
	for _, m := range members {
		if matchFn(m) {
			out = append(out, m)
		}
	}
	// COUNT counts members, i.e. one array element each.
	scanPageReply(c.w, out, start, count)
	return nil
}
