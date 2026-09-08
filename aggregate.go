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

// writeAggCount was removed: the sparse header is now written atomically together
// with the element writes in the same batch (see saveAgg / hashSetSparse /
// setAddSparse / zAddSparse), so a sparse collection always commits as one unit.

func (s *Store) scanHash(db uint16, key string) (map[string][]byte, error) {
	s.aggMu.RLock()
	defer s.aggMu.RUnlock()
	out := map[string][]byte{}
	prefix := storage.ElemPrefix(storage.SegHash, db, key)
	err := s.eng.Scan(prefix, func(k, v []byte) error {
		out[storage.ElemFromKey(prefix, k)] = append([]byte(nil), v...)
		return nil
	})
	return out, err
}

func (s *Store) scanSet(db uint16, key string) (map[string]struct{}, error) {
	s.aggMu.RLock()
	defer s.aggMu.RUnlock()
	out := map[string]struct{}{}
	prefix := storage.ElemPrefix(storage.SegSet, db, key)
	err := s.eng.ScanKeys(prefix, func(k []byte) error {
		out[storage.ElemFromKey(prefix, k)] = struct{}{}
		return nil
	})
	return out, err
}

func (s *Store) scanZSet(db uint16, key string) (map[string]float64, error) {
	s.aggMu.RLock()
	defer s.aggMu.RUnlock()
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
	s.aggMu.RLock()
	defer s.aggMu.RUnlock()
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
//
// A sparse collection is stored as one key per element plus a header. A full read
// (HGETALL, HSCAN, SMEMBERS, ZRANGE, ...) reconstructs it by scanning every
// element key, so a reader must never observe a half-written collection. To
// guarantee that, every element write and the header are staged into a SINGLE
// batch and committed atomically: from any reader's perspective the collection
// flips from the old state to the new one in one step, never a torn in-between.
func (s *Store) saveAgg(db uint16, key string, a *agg) error {
	// Serialise against a concurrent element scan (readers hold RLock). The whole
	// commit below - elements and header - is one atomic batch, but the scan must
	// not observe it mid-apply.
	s.aggMu.Lock()
	defer s.aggMu.Unlock()

	wantSparse := !fitsInline(a)

	// Inline stays inline: one key, one atomic batch. When demoting from sparse
	// we drop the per-element keys and write the inline value in the same batch.
	if !wantSparse {
		batch := s.eng.Batch()
		if a.sparse {
			if err := dropElemsBatch(batch, db, key, a.typ); err != nil {
				batch.Close()
				return err
			}
			a.sparse = false
		}
		payload := encodeInline(a)
		if err := putValueBatch(batch, db, key, a.typ, payload, a.expireAt, a.origExpire); err != nil {
			batch.Close()
			return err
		}
		if err := s.eng.Apply(batch); err != nil {
			batch.Close()
			return err
		}
		batch.Close()
		s.dict.Set(db, key, a.typ, a.expireAt, payload)
		return nil
	}

	// Sparse: stage every element write and the header into one batch so the
	// whole collection commits atomically.
	batch := s.eng.Batch()
	seg, _ := storage.SegmentFor(a.typ)
	if !a.sparse {
		// Promote from inline: everything is new.
		a.sparse = true
		if err := writeElemsBatch(batch, seg, db, key, a); err != nil {
			batch.Close()
			return err
		}
	} else {
		if err := diffElemsBatch(batch, seg, db, key, a); err != nil {
			batch.Close()
			return err
		}
	}
	header := storage.EncodeSparseHeader(a.typ, a.len(), a.head, a.tail)
	if err := putValueBatch(batch, db, key, a.typ, header, a.expireAt, a.origExpire); err != nil {
		batch.Close()
		return err
	}
	if err := s.eng.Apply(batch); err != nil {
		batch.Close()
		return err
	}
	batch.Close()
	s.dict.Set(db, key, a.typ, a.expireAt, header)
	return nil
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

// putValueBatch stages a value write plus the matching expiry-index maintenance
// into an existing batch (mirroring putValue, but without committing). The header
// of a sparse aggregate is just another value, so this lets saveAgg and the
// incremental sparse writers commit elements and header as one atomic unit.
func putValueBatch(batch *storage.Batch, db uint16, key string, typ uint8, val []byte, expireAtMs, prevExpire int64) error {
	if err := batch.Set(storage.EncodeDataKey(db, key), storage.EncodeValue(typ, expireAtMs, val)); err != nil {
		return err
	}
	if prevExpire != 0 && prevExpire != expireAtMs {
		if err := batch.Delete(storage.EncodeExpireKey(prevExpire, db, key)); err != nil {
			return err
		}
	}
	if expireAtMs != 0 && expireAtMs != prevExpire {
		if err := batch.Set(storage.EncodeExpireKey(expireAtMs, db, key), nil); err != nil {
			return err
		}
	}
	return nil
}

// dropElemsBatch stages the deletion of every per-element key of a collection
// into an existing batch.
func dropElemsBatch(batch *storage.Batch, db uint16, key string, typ byte) error {
	seg, ok := storage.SegmentFor(typ)
	if !ok {
		return nil
	}
	prefix := storage.ElemPrefix(seg, db, key)
	if err := batch.DeleteRange(prefix, prefixEnd(prefix)); err != nil {
		return err
	}
	if typ == config.TypeZSet {
		sp := storage.ScorePrefix(db, key)
		if err := batch.DeleteRange(sp, prefixEnd(sp)); err != nil {
			return err
		}
	}
	return nil
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

// writeElemsBatch stages every element of a sparse collection into batch.
func writeElemsBatch(batch *storage.Batch, seg byte, db uint16, key string, a *agg) error {
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
	return nil
}

// diffElemsBatch stages only the elements that changed relative to the loaded
// snapshot into batch.
func diffElemsBatch(batch *storage.Batch, seg byte, db uint16, key string, a *agg) error {
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
	return nil
}

// dropElems removes every per-element key of a collection being demoted or deleted.
func (s *Store) dropElems(db uint16, key string, typ byte) error {
	batch := s.eng.Batch()
	defer batch.Close()
	if err := dropElemsBatch(batch, db, key, typ); err != nil {
		return err
	}
	return s.eng.Apply(batch)
}

// deleteAgg removes an aggregate key and all of its elements.
func (s *Store) deleteAgg(db uint16, key string, typ byte) error {
	// deleteKey takes aggMu.Lock and drops the per-element keys as well.
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
