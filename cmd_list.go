package redistore

import (
	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/storage"
)

// List commands.
//
// Sparse lists keep a sequence number per element, so a push or pop at either
// end is a single key write regardless of length - the reason a list is not
// stored as one blob.

func (a *agg) ensureList() {
	if a.list == nil {
		a.list = []string{}
	}
}

// listPush prepends (left) or appends (right) elements and returns the new
// length. With onlyIfExists set, a missing key is left alone (LPUSHX/RPUSHX).
func (s *Store) listPush(db uint16, key string, elems []string, left, onlyIfExists bool) (int64, error) {
	n, err := s.listPushUnnotified(db, key, elems, left, onlyIfExists)
	if err == nil && n > 0 {
		// Wake anything blocked on this list; without this a BLPOP would sit
		// until its timeout even though data just arrived.
		s.notifyListChanged(db, key)
	}
	return n, err
}

func (s *Store) listPushUnnotified(db uint16, key string, elems []string, left, onlyIfExists bool) (int64, error) {
	a, err := s.loadAgg(db, key, config.TypeList)
	if err != nil {
		return 0, err
	}
	if onlyIfExists && !a.exists {
		return 0, nil
	}
	a.ensureList()

	if !a.sparse && len(a.list)+len(elems) <= storage.InlineMaxEntries && allShort(elems, storage.InlineMaxValueSize) {
		if left {
			rev := make([]string, len(elems))
			for i, e := range elems {
				rev[len(elems)-1-i] = e
			}
			a.list = append(rev, a.list...)
		} else {
			a.list = append(a.list, elems...)
		}
		if err := s.saveAgg(db, key, a); err != nil {
			return 0, err
		}
		return int64(len(a.list)), nil
	}
	return s.listPushSparse(db, key, a, elems, left)
}

func (s *Store) listPushSparse(db uint16, key string, a *agg, elems []string, left bool) (int64, error) {
	if !a.sparse {
		// Promote: materialise the current elements under fresh sequence numbers.
		if err := s.writeAggElems(db, key, a, nil); err != nil {
			return 0, err
		}
	}
	batch := s.eng.Batch()
	defer batch.Close()

	for _, e := range elems {
		var seq int64
		if left {
			seq = a.head - 1
			a.head = seq
		} else {
			seq = a.tail + 1
			a.tail = seq
		}
		if err := batch.Set(storage.EncodeListSeqKey(db, key, seq), []byte(e)); err != nil {
			return 0, err
		}
	}
	if err := s.eng.Apply(batch); err != nil {
		return 0, err
	}
	a.sparse = true
	a.list = append(a.list, elems...)
	if err := s.writeAggHeader(db, key, a); err != nil {
		return 0, err
	}
	return int64(len(a.list)), nil
}

// listPop removes and returns up to n elements from one end.
func (s *Store) listPop(db uint16, key string, n int, left bool) ([]string, error) {
	a, err := s.loadAgg(db, key, config.TypeList)
	if err != nil {
		return nil, err
	}
	if !a.exists || len(a.list) == 0 {
		return nil, nil
	}
	if n > len(a.list) {
		n = len(a.list)
	}

	var out []string
	if left {
		out = a.list[:n:n]
		a.list = a.list[n:]
	} else {
		out = a.list[len(a.list)-n:]
		a.list = a.list[:len(a.list)-n]
	}

	if !a.sparse {
		if len(a.list) == 0 {
			if _, err := s.deleteKey(db, key); err != nil {
				return nil, err
			}
			return out, nil
		}
		if err := s.saveAgg(db, key, a); err != nil {
			return nil, err
		}
		return out, nil
	}

	// Sparse: drop the sequence keys that were taken.
	batch := s.eng.Batch()
	defer batch.Close()
	if left {
		for range n {
			if err := batch.Delete(storage.EncodeListSeqKey(db, key, a.head)); err != nil {
				return nil, err
			}
			a.head++
		}
	} else {
		for range n {
			if err := batch.Delete(storage.EncodeListSeqKey(db, key, a.tail)); err != nil {
				return nil, err
			}
			a.tail--
		}
	}
	if err := s.eng.Apply(batch); err != nil {
		return nil, err
	}
	if len(a.list) == 0 {
		if _, err := s.deleteKey(db, key); err != nil {
			return nil, err
		}
		return out, nil
	}
	return out, s.writeAggHeader(db, key, a)
}

func (s *Store) listRange(db uint16, key string, start, stop int) ([]string, error) {
	a, err := s.loadAgg(db, key, config.TypeList)
	if err != nil {
		return nil, err
	}
	return sliceRange(a.list, start, stop), nil
}

// sliceRange applies Redis' inclusive, negative-aware index pair to a slice.
func sliceRange(items []string, start, stop int) []string {
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
	out := make([]string, stop-start+1)
	copy(out, items[start:stop+1])
	return out
}

func (s *Store) listSet(db uint16, key string, index int, val string) error {
	a, err := s.loadAgg(db, key, config.TypeList)
	if err != nil {
		return err
	}
	if !a.exists {
		return ErrNoSuchKey
	}
	idx := index
	if idx < 0 {
		idx += len(a.list)
	}
	if idx < 0 || idx >= len(a.list) {
		return &protoError{"ERR index out of range"}
	}
	a.list[idx] = val
	if a.sparse {
		seq := a.seqs[idx]
		if err := s.eng.Put(storage.EncodeListSeqKey(db, key, seq), []byte(val)); err != nil {
			return err
		}
		return nil
	}
	return s.saveAgg(db, key, a)
}

// listRem removes count occurrences of value.
func (s *Store) listRem(db uint16, key string, count int, val string) (int64, error) {
	a, err := s.loadAgg(db, key, config.TypeList)
	if err != nil {
		return 0, err
	}
	if !a.exists {
		return 0, nil
	}
	var removed []int
	if count >= 0 {
		limit := count
		for i, e := range a.list {
			if e == val {
				removed = append(removed, i)
				if limit > 0 && len(removed) >= limit {
					break
				}
			}
		}
	} else {
		limit := -count
		for i := len(a.list) - 1; i >= 0; i-- {
			if a.list[i] == val {
				removed = append(removed, i)
				if limit > 0 && len(removed) >= limit {
					break
				}
			}
		}
	}
	if len(removed) == 0 {
		return 0, nil
	}
	if a.sparse {
		batch := s.eng.Batch()
		defer batch.Close()
		for _, i := range removed {
			if err := batch.Delete(storage.EncodeListSeqKey(db, key, a.seqs[i])); err != nil {
				return 0, err
			}
		}
		if err := s.eng.Apply(batch); err != nil {
			return 0, err
		}
	}
	keep := make([]string, 0, len(a.list)-len(removed))
	drop := map[int]bool{}
	for _, i := range removed {
		drop[i] = true
	}
	// seqs must stay parallel to list.
	if a.sparse {
		var keptSeqs []int64
		for i, e := range a.list {
			if !drop[i] {
				keep = append(keep, e)
				keptSeqs = append(keptSeqs, a.seqs[i])
			}
		}
		a.seqs = keptSeqs
	} else {
		for i, e := range a.list {
			if !drop[i] {
				keep = append(keep, e)
			}
		}
	}
	a.list = keep
	if len(a.list) == 0 {
		if _, err := s.deleteKey(db, key); err != nil {
			return 0, err
		}
		return int64(len(removed)), nil
	}
	if err := s.saveAgg(db, key, a); err != nil {
		return 0, err
	}
	return int64(len(removed)), nil
}

func (s *Store) listTrim(db uint16, key string, start, stop int) error {
	a, err := s.loadAgg(db, key, config.TypeList)
	if err != nil {
		return err
	}
	if !a.exists {
		return nil
	}
	kept := sliceRange(a.list, start, stop)
	if len(kept) == len(a.list) {
		return nil
	}
	if len(kept) == 0 {
		_, err := s.deleteKey(db, key)
		return err
	}
	// Rebuilding is simplest and LTRIM is inherently O(N) anyway.
	a.list = kept
	if a.sparse {
		if err := s.dropElems(db, key, config.TypeList); err != nil {
			return err
		}
		a.sparse = false
		a.seqs = nil
		a.head, a.tail = 0, 0
	}
	return s.saveAgg(db, key, a)
}

func cmdLPush(c *Ctx, args [][]byte) error  { return pushCmd(c, args, true, false) }
func cmdRPush(c *Ctx, args [][]byte) error  { return pushCmd(c, args, false, false) }
func cmdLPushX(c *Ctx, args [][]byte) error { return pushCmd(c, args, true, true) }
func cmdRPushX(c *Ctx, args [][]byte) error { return pushCmd(c, args, false, true) }

func pushCmd(c *Ctx, args [][]byte, left, x bool) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	elems := byteSliceToStrings(args[1:])
	n, err := c.Store.listPush(c.DB, string(args[0]), elems, left, x)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdLPop(c *Ctx, args [][]byte) error { return popCmd(c, args, true) }
func cmdRPop(c *Ctx, args [][]byte) error { return popCmd(c, args, false) }

func popCmd(c *Ctx, args [][]byte, left bool) error {
	if len(args) < 1 || len(args) > 2 {
		return WrongArgs("lpop")
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
	out, err := c.Store.listPop(c.DB, string(args[0]), n, left)
	if err != nil {
		return err
	}
	if n == 1 {
		if len(out) == 0 {
			c.writeNull()
		} else {
			c.w.WriteBulkString(out[0])
		}
		return nil
	}
	c.w.WriteArray(len(out))
	for _, e := range out {
		c.w.WriteBulkString(e)
	}
	return nil
}

func cmdLRange(c *Ctx, args [][]byte) error {
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
	out, err := c.Store.listRange(c.DB, string(args[0]), int(start), int(stop))
	if err != nil {
		return err
	}
	c.w.WriteArray(len(out))
	for _, e := range out {
		c.w.WriteBulkString(e)
	}
	return nil
}

func cmdLLen(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeList)
	if err != nil {
		return err
	}
	c.writeInt(int64(len(a.list)))
	return nil
}

func cmdLIndex(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	idx, err := toInt64(args[1])
	if err != nil {
		return err
	}
	a, err := c.Store.loadAgg(c.DB, string(args[0]), config.TypeList)
	if err != nil {
		return err
	}
	if !a.exists {
		c.writeNull()
		return nil
	}
	i := int(idx)
	if i < 0 {
		i += len(a.list)
	}
	if i < 0 || i >= len(a.list) {
		c.writeNull()
		return nil
	}
	c.w.WriteBulkString(a.list[i])
	return nil
}

func cmdLSet(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	idx, err := toInt64(args[1])
	if err != nil {
		return err
	}
	if err := c.Store.listSet(c.DB, string(args[0]), int(idx), string(args[2])); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

func cmdLRem(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	count, err := toInt64(args[1])
	if err != nil {
		return err
	}
	n, err := c.Store.listRem(c.DB, string(args[0]), int(count), string(args[2]))
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdLTrim(c *Ctx, args [][]byte) error {
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
	if err := c.Store.listTrim(c.DB, string(args[0]), int(start), int(stop)); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

// cmdRPopLPush pops from the tail of src and pushes to the head of dst,
// returning the element moved.
func cmdRPopLPush(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	src, dst := string(args[0]), string(args[1])
	out, err := c.Store.listPop(c.DB, src, 1, false)
	if err != nil {
		return err
	}
	if len(out) == 0 {
		c.writeNull()
		return nil
	}
	if _, err := c.Store.listPush(c.DB, dst, []string{out[0]}, true, false); err != nil {
		return err
	}
	c.w.WriteBulkString(out[0])
	return nil
}

func byteSliceToStrings(in [][]byte) []string {
	out := make([]string, len(in))
	for i, b := range in {
		out[i] = string(b)
	}
	return out
}

func allShort(items []string, max int) bool {
	for _, s := range items {
		if len(s) > max {
			return false
		}
	}
	return true
}
