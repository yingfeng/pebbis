package redistore

import (
	"sort"

	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/storage"
)

// Read/write primitives for the aggregate types.
//
// Every aggregate has two representations (see storage/object.go): inline for
// small collections, sparse (one key per element) for large ones. Commands work
// against the in-memory form and hand it back to saveAggregate, which picks the
// representation and - for sparse collections - writes only what changed.

// agg is the in-memory view of an aggregate value.
type agg struct {
	typ byte
	// exists reports whether the key was found.
	exists bool
	// sparse is the representation currently used on disk.
	sparse bool
	// expireAt is the key's TTL in unix ms (0 for none). Commands may change
	// it; origExpire is what was on disk and anchors the expiry index.
	expireAt   int64
	origExpire int64

	hash map[string][]byte
	set  map[string]struct{}
	zset map[string]float64
	list []string

	// seqs holds the sequence numbers of a sparse list, parallel to list.
	seqs []int64
	// head/tail bound the sequence numbers of a sparse list.
	head, tail int64

	// orig* hold the loaded snapshot so saveAggregate can diff.
	origHash map[string][]byte
	origSet  map[string]struct{}
	origZSet map[string]float64
}

// loadAgg reads an aggregate key. It fails with ErrWrongType when the key holds
// a string, and reports exists=false for a missing key.
func (s *Store) loadAgg(db uint16, key string, typ byte) (*agg, error) {
	payload, gotTyp, expireAt, ok, err := s.getTyped(db, key)
	if err != nil {
		return nil, err
	}
	a := &agg{typ: typ, expireAt: expireAt, origExpire: expireAt}
	if !ok {
		return a, nil
	}
	if gotTyp != typ {
		return nil, ErrWrongType
	}
	a.exists = true

	obj, err := storage.DecodeObjectHeader(payload)
	if err != nil {
		return nil, err
	}
	a.sparse = obj.Enc == storage.EncSparse
	a.head, a.tail = obj.Head, obj.Tail

	switch typ {
	case config.TypeHash:
		if a.sparse {
			a.hash, err = s.scanHash(db, key)
		} else {
			a.hash, err = storage.DecodeHashInline(obj.Data)
		}
		if err == nil {
			a.origHash = cloneHash(a.hash)
		}
	case config.TypeSet:
		if a.sparse {
			a.set, err = s.scanSet(db, key)
		} else {
			a.set, err = storage.DecodeSetInline(obj.Data)
		}
		if err == nil {
			a.origSet = cloneSet(a.set)
		}
	case config.TypeZSet:
		if a.sparse {
			a.zset, err = s.scanZSet(db, key)
		} else {
			a.zset, err = storage.DecodeZSetInline(obj.Data)
		}
		if err == nil {
			a.origZSet = cloneZSet(a.zset)
		}
	case config.TypeList:
		if a.sparse {
			a.list, a.seqs, err = s.scanList(db, key)
		} else {
			a.list, err = storage.DecodeListInline(obj.Data)
		}
	}
	return a, err
}

// aggEncoding reads only an aggregate's header, without materialising its
// elements. Point writes on a sparse collection go through here: loading the
// whole collection just to add one element would make HSET/SADD/ZADD quadratic
// in the collection size.
func (s *Store) aggEncoding(db uint16, key string, typ byte) (obj storage.Object, expireAt int64, exists bool, err error) {
	payload, gotTyp, exp, ok, err := s.getTyped(db, key)
	if err != nil || !ok {
		return storage.Object{}, 0, false, err
	}
	if gotTyp != typ {
		return storage.Object{}, 0, false, ErrWrongType
	}
	obj, err = storage.DecodeObjectHeader(payload)
	if err != nil {
		return storage.Object{}, 0, false, err
	}
	return obj, exp, true, nil
}

// writeAggCount updates just the element counter in a sparse header.
func (s *Store) writeAggCount(db uint16, key string, typ byte, count int, head, tail, expireAt, origExpire int64) error {
	payload := storage.EncodeSparseHeader(typ, count, head, tail)
	return s.putValue(db, key, typ, payload, expireAt, origExpire)
}

func (s *Store) scanHash(db uint16, key string) (map[string][]byte, error) {
	out := map[string][]byte{}
	prefix := storage.ElemPrefix(storage.SegHash, db, key)
	err := s.eng.Scan(prefix, func(k, v []byte) error {
		out[storage.ElemFromKey(prefix, k)] = append([]byte(nil), v...)
		return nil
	})
	return out, err
}

func (s *Store) scanSet(db uint16, key string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	prefix := storage.ElemPrefix(storage.SegSet, db, key)
	err := s.eng.ScanKeys(prefix, func(k []byte) error {
		out[storage.ElemFromKey(prefix, k)] = struct{}{}
		return nil
	})
	return out, err
}

func (s *Store) scanZSet(db uint16, key string) (map[string]float64, error) {
	out := map[string]float64{}
	prefix := storage.ElemPrefix(storage.SegZSetM, db, key)
	err := s.eng.Scan(prefix, func(k, v []byte) error {
		if len(v) < 8 {
			return nil
		}
		var u uint64
		for i := range 8 {
			u = u<<8 | uint64(v[i])
		}
		out[storage.ElemFromKey(prefix, k)] = storage.DecodeScore(u)
		return nil
	})
	return out, err
}

func (s *Store) scanList(db uint16, key string) ([]string, []int64, error) {
	var elems []string
	var seqs []int64
	prefix := storage.ElemPrefix(storage.SegList, db, key)
	err := s.eng.Scan(prefix, func(k, v []byte) error {
		rest := k[len(prefix):]
		if len(rest) < 8 {
			return nil
		}
		var u uint64
		for i := range 8 {
			u = u<<8 | uint64(rest[i])
		}
		seqs = append(seqs, int64(u))
		elems = append(elems, string(v))
		return nil
	})
	return elems, seqs, err
}

// saveAgg writes the aggregate back, choosing the representation and writing
// only the changed elements when the collection is sparse.
func (s *Store) saveAgg(db uint16, key string, a *agg) error {
	wantSparse := !fitsInline(a)

	// Inline stays inline: one key, one write.
	if !wantSparse {
		if a.sparse {
			// Convert back down: drop the per-element keys.
			if err := s.dropElems(db, key, a.typ); err != nil {
				return err
			}
			a.sparse = false
		}
		payload := encodeInline(a)
		return s.putValue(db, key, a.typ, payload, a.expireAt, a.origExpire)
	}

	// Sparse: write the header plus the delta.
	if !a.sparse {
		// Promote from inline: everything is new.
		a.sparse = true
		if err := s.writeAggElems(db, key, a, nil); err != nil {
			return err
		}
	} else {
		if err := s.diffAggElems(db, key, a); err != nil {
			return err
		}
	}
	return s.writeAggHeader(db, key, a)
}

// fitsInline reports whether the collection should stay in the inline form.
func fitsInline(a *agg) bool {
	switch a.typ {
	case config.TypeHash:
		if len(a.hash) > storage.InlineMaxEntries {
			return false
		}
		for _, v := range a.hash {
			if len(v) > storage.InlineMaxValueSize {
				return false
			}
		}
		return true
	case config.TypeSet:
		return len(a.set) <= storage.InlineMaxEntries
	case config.TypeZSet:
		return len(a.zset) <= storage.InlineMaxEntries
	case config.TypeList:
		if len(a.list) > storage.InlineMaxEntries {
			return false
		}
		for _, e := range a.list {
			if len(e) > storage.InlineMaxValueSize {
				return false
			}
		}
		return true
	}
	return true
}

func encodeInline(a *agg) []byte {
	switch a.typ {
	case config.TypeHash:
		fields := sortedKeys(a.hash)
		return storage.EncodeHashInline(fields, func(f string) []byte { return a.hash[f] })
	case config.TypeSet:
		members := make([]string, 0, len(a.set))
		for m := range a.set {
			members = append(members, m)
		}
		sort.Strings(members)
		return storage.EncodeSetInline(members)
	case config.TypeZSet:
		members := make([]storage.Member, 0, len(a.zset))
		for m, sc := range a.zset {
			members = append(members, storage.Member{Member: m, Score: sc})
		}
		sort.Slice(members, func(i, j int) bool { return members[i].Member < members[j].Member })
		return storage.EncodeZSetInline(members)
	case config.TypeList:
		return storage.EncodeListInline(a.list)
	}
	return nil
}

// writeAggHeader persists the sparse header and refreshes the index.
func (s *Store) writeAggHeader(db uint16, key string, a *agg) error {
	count := a.len()
	payload := storage.EncodeSparseHeader(a.typ, count, a.head, a.tail)
	return s.putValue(db, key, a.typ, payload, a.expireAt, a.origExpire)
}

func (a *agg) len() int {
	switch a.typ {
	case config.TypeHash:
		return len(a.hash)
	case config.TypeSet:
		return len(a.set)
	case config.TypeZSet:
		return len(a.zset)
	case config.TypeList:
		return len(a.list)
	}
	return 0
}

// writeAggElems writes every element of a freshly promoted sparse collection.
func (s *Store) writeAggElems(db uint16, key string, a *agg, _ map[string]bool) error {
	batch := s.eng.Batch()
	defer batch.Close()
	seg, _ := storage.SegmentFor(a.typ)

	switch a.typ {
	case config.TypeHash:
		for f, v := range a.hash {
			if err := batch.Set(storage.EncodeElemKey(seg, db, key, f), v); err != nil {
				return err
			}
		}
	case config.TypeSet:
		for m := range a.set {
			if err := batch.Set(storage.EncodeElemKey(seg, db, key, m), nil); err != nil {
				return err
			}
		}
	case config.TypeZSet:
		for m, sc := range a.zset {
			if err := batch.Set(storage.EncodeElemKey(seg, db, key, m), scoreBytes(sc)); err != nil {
				return err
			}
			if err := batch.Set(storage.EncodeScoreKey(db, key, sc, m), nil); err != nil {
				return err
			}
		}
	case config.TypeList:
		// Sequence numbers start centred so that LPUSH can go negative.
		a.head, a.tail = 0, int64(len(a.list))-1
		if len(a.list) == 0 {
			a.head, a.tail = 0, -1
		}
		a.seqs = make([]int64, len(a.list))
		for i, e := range a.list {
			seq := a.head + int64(i)
			a.seqs[i] = seq
			if err := batch.Set(storage.EncodeListSeqKey(db, key, seq), []byte(e)); err != nil {
				return err
			}
		}
		if len(a.list) > 0 {
			a.tail = a.head + int64(len(a.list)) - 1
		}
	}
	return s.eng.Apply(batch)
}

// diffAggElems writes only what changed relative to the loaded snapshot.
func (s *Store) diffAggElems(db uint16, key string, a *agg) error {
	batch := s.eng.Batch()
	defer batch.Close()
	seg, _ := storage.SegmentFor(a.typ)

	switch a.typ {
	case config.TypeHash:
		for f := range a.origHash {
			if _, ok := a.hash[f]; !ok {
				if err := batch.Delete(storage.EncodeElemKey(seg, db, key, f)); err != nil {
					return err
				}
			}
		}
		for f, v := range a.hash {
			old, had := a.origHash[f]
			if had && bytesEqual(old, v) {
				continue
			}
			if err := batch.Set(storage.EncodeElemKey(seg, db, key, f), v); err != nil {
				return err
			}
		}
	case config.TypeSet:
		for m := range a.origSet {
			if _, ok := a.set[m]; !ok {
				if err := batch.Delete(storage.EncodeElemKey(seg, db, key, m)); err != nil {
					return err
				}
			}
		}
		for m := range a.set {
			if _, had := a.origSet[m]; had {
				continue
			}
			if err := batch.Set(storage.EncodeElemKey(seg, db, key, m), nil); err != nil {
				return err
			}
		}
	case config.TypeZSet:
		for m, oldScore := range a.origZSet {
			if _, ok := a.zset[m]; !ok {
				if err := batch.Delete(storage.EncodeElemKey(seg, db, key, m)); err != nil {
					return err
				}
				if err := batch.Delete(storage.EncodeScoreKey(db, key, oldScore, m)); err != nil {
					return err
				}
			}
		}
		for m, sc := range a.zset {
			old, had := a.origZSet[m]
			if had && old == sc {
				continue
			}
			if had {
				if err := batch.Delete(storage.EncodeScoreKey(db, key, old, m)); err != nil {
					return err
				}
			}
			if err := batch.Set(storage.EncodeElemKey(seg, db, key, m), scoreBytes(sc)); err != nil {
				return err
			}
			if err := batch.Set(storage.EncodeScoreKey(db, key, sc, m), nil); err != nil {
				return err
			}
		}
	case config.TypeList:
		// Lists are edited structurally by push/pop, which write their own
		// deltas; nothing to do here.
		return nil
	}
	return s.eng.Apply(batch)
}

// dropElems removes every per-element key of a collection being demoted.
func (s *Store) dropElems(db uint16, key string, typ byte) error {
	seg, ok := storage.SegmentFor(typ)
	if !ok {
		return nil
	}
	prefix := storage.ElemPrefix(seg, db, key)
	if err := s.eng.DeleteRange(prefix, prefixEnd(prefix)); err != nil {
		return err
	}
	if typ == config.TypeZSet {
		sp := storage.ScorePrefix(db, key)
		if err := s.eng.DeleteRange(sp, prefixEnd(sp)); err != nil {
			return err
		}
	}
	return nil
}

// deleteAgg removes an aggregate key and all of its elements.
func (s *Store) deleteAgg(db uint16, key string, typ byte) error {
	if err := s.dropElems(db, key, typ); err != nil {
		return err
	}
	_, err := s.deleteKey(db, key)
	return err
}

func scoreBytes(f float64) []byte {
	b := make([]byte, 8)
	u := storage.EncodeScore(f)
	for i := range 8 {
		b[i] = byte(u >> (56 - 8*i))
	}
	return b
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortedKeys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func cloneHash(m map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneSet(m map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}

func cloneZSet(m map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
