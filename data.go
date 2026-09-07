package redistore

import (
	"time"

	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/storage"
)

// This file holds the data-path primitives shared by the command handlers and
// the embedded API. Everything here is network-free: commands in cmd_*.go turn
// these results into RESP, api.go exposes them as ordinary Go values.

// getTyped returns the payload stored at key, its type and its expiry.
//
// The dict is a cache: a miss there does not mean the key is absent, only that
// it is cold (never loaded, or evicted). The authoritative copy is Pebble, so a
// miss falls through to a Get and re-admits the entry on the way out.
func (s *Store) getTyped(db uint16, key string) (payload []byte, typ uint8, expireAt int64, ok bool, err error) {
	now := s.clock.NowMilli()

	if e, found := s.dict.Lookup(db, key); found {
		if inl := e.Inline(); len(inl) > 0 {
			s.stats.hits.Add(1)
			s.touch(db, key)
			return inl, e.Type(), e.Expiry(), true, nil
		}
	}

	v, release, err := s.eng.Get(storage.EncodeDataKey(db, key))
	if err == storage.ErrNotFound {
		s.stats.misses.Add(1)
		return nil, 0, 0, false, nil
	}
	if err != nil {
		return nil, 0, 0, false, err
	}
	defer release()

	typ, expireAt, payload, err = storage.DecodeValue(v)
	if err != nil {
		return nil, 0, 0, false, err
	}
	if expireAt != 0 && expireAt <= now {
		// Lazy expiry: the key is logically gone, so remove it everywhere.
		if err := s.deleteExpired(db, key, expireAt); err != nil {
			return nil, 0, 0, false, err
		}
		s.stats.misses.Add(1)
		s.stats.expired.Add(1)
		return nil, 0, 0, false, nil
	}

	// Re-admit into the index: this covers both lazy startup and re-access
	// after an eviction in persist mode.
	s.dict.Set(db, key, typ, expireAt, payload)
	s.stats.hits.Add(1)
	s.touch(db, key)

	// payload aliases Pebble memory that release() invalidates, so hand back a
	// copy.
	out := make([]byte, len(payload))
	copy(out, payload)
	return out, typ, expireAt, true, nil
}

// getString returns the string value at key.
func (s *Store) getString(db uint16, key string) ([]byte, bool, error) {
	payload, typ, _, ok, err := s.getTyped(db, key)
	if err != nil || !ok {
		return nil, false, err
	}
	if typ != config.TypeString {
		return nil, false, ErrWrongType
	}
	return payload, true, nil
}

// oldExpiry returns the expiry currently recorded for key, or 0 for none.
func (s *Store) oldExpiry(db uint16, key string) int64 {
	if e, ok := s.dict.Lookup(db, key); ok {
		return e.Expiry()
	}
	v, release, err := s.eng.Get(storage.EncodeDataKey(db, key))
	if err != nil {
		return 0
	}
	defer release()
	_, expireAt, _, err := storage.DecodeValue(v)
	if err != nil {
		return 0
	}
	return expireAt
}

// setString writes a string value, honouring the SET option grammar.
// It returns the previous value when opt.GET is set, and whether the write
// actually happened (NX/XX may reject it).
func (s *Store) setString(db uint16, key string, val []byte, opt SetOption) (old []byte, existed bool, written bool, err error) {
	now := s.clock.Now()
	prev, prevExpire, exists, err := s.lookupForWrite(db, key)
	if err != nil {
		return nil, false, false, err
	}
	if exists {
		old = prev
	}
	if opt.NX && exists {
		return old, exists, false, nil
	}
	if opt.XX && !exists {
		return old, exists, false, nil
	}

	var expireAtMs int64
	switch {
	case opt.KEEPTTL:
		expireAtMs = prevExpire
	default:
		if at := opt.ExpireAt(now); !at.IsZero() {
			expireAtMs = at.UnixMilli()
		}
	}

	// A future expiry must not be turned into a past one silently.
	if expireAtMs != 0 && expireAtMs <= now.UnixMilli() {
		// Redis treats "expire in the past" as an immediate delete.
		if _, err := s.deleteKey(db, key); err != nil {
			return old, exists, false, err
		}
		s.stats.keyChanges.Add(1)
		return old, exists, true, nil
	}

	if err := s.putValue(db, key, config.TypeString, val, expireAtMs, prevExpire); err != nil {
		return old, exists, false, err
	}
	s.stats.keyChanges.Add(1)
	s.evictor.MaybeEvict()
	return old, exists, true, nil
}

// setStringSimple is the fast path used by the embedded API and by commands
// that carry no options.
func (s *Store) setStringSimple(db uint16, key string, val []byte, expireAtMs int64) error {
	prevExpire := s.oldExpiry(db, key)
	if err := s.putValue(db, key, config.TypeString, val, expireAtMs, prevExpire); err != nil {
		return err
	}
	s.stats.keyChanges.Add(1)
	s.evictor.MaybeEvict()
	return nil
}

// lookupForWrite returns the existing value, its expiry, and whether the key
// exists. Used by commands that need the previous value (GETSET, INCR, APPEND).
func (s *Store) lookupForWrite(db uint16, key string) (val []byte, expireAt int64, ok bool, err error) {
	payload, _, exp, found, err := s.getTyped(db, key)
	if err != nil || !found {
		return nil, 0, false, err
	}
	return payload, exp, true, nil
}

// putValue writes the value and keeps the expiry index in step, all in one
// atomic batch. prevExpire is the expiry the key had before, whose index entry
// has to be dropped when it changes.
func (s *Store) putValue(db uint16, key string, typ uint8, val []byte, expireAtMs, prevExpire int64) error {
	batch := s.eng.Batch()
	defer batch.Close()

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
	if err := s.eng.Apply(batch); err != nil {
		return err
	}

	// Pebble is the source of truth, so the index is updated only after the
	// write is durable: a crash between the two leaves data on disk that the
	// next startup will index.
	s.dict.Set(db, key, typ, expireAtMs, val)
	return nil
}

// deleteKey removes a key from both the index and Pebble. Aggregates also drop
// their per-element keys, otherwise the elements would outlive the key itself.
func (s *Store) deleteKey(db uint16, key string) (bool, error) {
	typ, found, err := s.typeOf(db, key)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	if typ != config.TypeString {
		if err := s.dropElems(db, key, typ); err != nil {
			return false, err
		}
	}
	prevExpire := s.oldExpiry(db, key)
	s.dict.Delete(db, key)
	if err := s.deletePersisted(db, key); err != nil {
		return false, err
	}
	if prevExpire != 0 {
		if err := s.eng.Delete(storage.EncodeExpireKey(prevExpire, db, key)); err != nil {
			return false, err
		}
	}
	s.stats.keyChanges.Add(1)
	return true, nil
}

// deletePersisted removes only the value; the caller owns the expiry index.
func (s *Store) deletePersisted(db uint16, key string) error {
	return s.eng.Delete(storage.EncodeDataKey(db, key))
}

// deleteExpired removes a key that has been discovered to be past its TTL.
func (s *Store) deleteExpired(db uint16, key string, expireAt int64) error {
	s.dict.Delete(db, key)
	batch := s.eng.Batch()
	defer batch.Close()
	if err := batch.Delete(storage.EncodeDataKey(db, key)); err != nil {
		return err
	}
	if expireAt != 0 {
		if err := batch.Delete(storage.EncodeExpireKey(expireAt, db, key)); err != nil {
			return err
		}
	}
	return s.eng.Apply(batch)
}

// exists counts how many of keys are present.
func (s *Store) exists(db uint16, keys []string) (int64, error) {
	var n int64
	for _, k := range keys {
		if _, _, _, ok, err := s.getTyped(db, k); err != nil {
			return 0, err
		} else if ok {
			n++
		}
	}
	return n, nil
}

// typeOf returns the object type tag of key, if it exists.
func (s *Store) typeOf(db uint16, key string) (uint8, bool, error) {
	_, typ, _, ok, err := s.getTyped(db, key)
	if err != nil || !ok {
		return 0, false, err
	}
	return typ, true, nil
}

// ttlOf returns the remaining TTL in milliseconds, or -1 (no TTL) / -2 (absent)
// using Redis' convention.
func (s *Store) ttlOf(db uint16, key string) int64 {
	_, _, expireAt, ok, err := s.getTyped(db, key)
	if err != nil || !ok {
		return -2
	}
	if expireAt == 0 {
		return -1
	}
	remain := expireAt - s.clock.NowMilli()
	if remain < 0 {
		remain = 0
	}
	return remain
}

// setExpiry applies an EXPIRE-family request.
func (s *Store) setExpiry(db uint16, key string, at time.Time, opt ExpireOption) (int64, error) {
	val, typ, curExpire, ok, err := s.getTyped(db, key)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	newExpire := at.UnixMilli()

	switch {
	case opt.NX && curExpire != 0:
		return 0, nil
	case opt.XX && curExpire == 0:
		return 0, nil
	case opt.GT && curExpire != 0 && newExpire <= curExpire:
		return 0, nil
	case opt.LT && curExpire != 0 && newExpire >= curExpire:
		return 0, nil
	}

	if newExpire <= s.clock.NowMilli() {
		// An expiry in the past deletes the key.
		if _, err := s.deleteKey(db, key); err != nil {
			return 0, err
		}
		return 1, nil
	}

	if err := s.putValue(db, key, typ, val, newExpire, curExpire); err != nil {
		return 0, err
	}
	return 1, nil
}

// persist drops the TTL of a key.
func (s *Store) persist(db uint16, key string) (bool, error) {
	val, typ, curExpire, ok, err := s.getTyped(db, key)
	if err != nil {
		return false, err
	}
	if !ok || curExpire == 0 {
		return false, nil
	}
	if err := s.putValue(db, key, typ, val, 0, curExpire); err != nil {
		return false, err
	}
	return true, nil
}

// rename moves a key, optionally only when the destination is free (NX).
func (s *Store) rename(db uint16, src, dst string, nx bool) error {
	val, typ, expireAt, ok, err := s.getTyped(db, src)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNoSuchKey
	}
	if nx {
		if _, _, _, exists, _ := s.getTyped(db, dst); exists {
			return nil
		}
	}
	if src == dst {
		return nil
	}
	dstExpire := s.oldExpiry(db, dst)
	if err := s.putValue(db, dst, typ, val, expireAt, dstExpire); err != nil {
		return err
	}
	if _, err := s.deleteKey(db, src); err != nil {
		return err
	}
	return nil
}

// flushDB drops every key in a logical database, from both the index and
// Pebble. Every segment has to be swept: aggregate elements live outside the
// data segment and would otherwise survive.
func (s *Store) flushDB(db uint16) error {
	s.dict.Flush(db)

	// Segments keyed as [tag][db]... can be cleared with a single range each:
	// [tag][db] is a prefix of everything belonging to this database, and
	// [tag][db+1] is the first key of the next one.
	for _, seg := range []byte{
		storage.SegHash, storage.SegList, storage.SegSet, storage.SegZSetM, storage.SegZSetS,
	} {
		lo := []byte{seg, byte(db >> 8), byte(db)}
		hi := []byte{seg, byte((db + 1) >> 8), byte(db + 1)}
		if err := s.eng.DeleteRange(lo, hi); err != nil {
			return err
		}
	}
	prefix := storage.DataPrefix(db)
	if err := s.eng.DeleteRange(prefix, prefixEnd(prefix)); err != nil {
		return err
	}

	// The expiry segment is ordered by time, not by database, so the entries
	// for this database have to be picked out one by one.
	batch := s.eng.Batch()
	defer batch.Close()
	err := s.eng.ScanKeys(storage.ExpirePrefix(), func(k []byte) error {
		_, edb, _, ok := storage.DecodeExpireKey(k)
		if !ok || edb != db {
			return nil
		}
		return batch.Delete(append([]byte(nil), k...))
	})
	if err != nil {
		return err
	}
	return s.eng.Apply(batch)
}

func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// keyAlive reports whether key exists and has not expired. It is the filter
// used by KEYS and SCAN, which must not resurrect expired keys.
func (s *Store) keyAlive(db uint16, key string, nowMs int64) bool {
	if e, ok := s.dict.Lookup(db, key); ok {
		return !e.Expired(nowMs)
	}
	_, _, _, ok, err := s.getTyped(db, key)
	return err == nil && ok
}

// touch records an access, but only when a policy needs the metadata.
func (s *Store) touch(db uint16, key string) {
	if s.cfg.TracksLRU() || s.cfg.TracksLFU() {
		s.dict.Touch(db, key)
	}
}
