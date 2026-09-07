package redistore

import (
	"time"

	"github.com/redistore/redistore/config"
)

// Embedded API.
//
// These methods bypass RESP entirely: no parsing, no connection, no allocation
// for the reply. They are the reason the store is usable as a library and not
// only as a server.

// Get returns the value at key in database 0, or nil when it is absent.
func (s *Store) Get(key string) ([]byte, error) {
	if err := s.checkAlive(); err != nil {
		return nil, err
	}
	val, _, err := s.getString(0, key)
	return val, err
}

// Set writes a value in database 0 with no expiry.
func (s *Store) Set(key string, val []byte) error {
	if err := s.checkAlive(); err != nil {
		return err
	}
	return s.setStringSimple(0, key, val, 0)
}

// SetEx writes a value with a TTL.
func (s *Store) SetEx(key string, val []byte, ttl time.Duration) error {
	if err := s.checkAlive(); err != nil {
		return err
	}
	return s.setStringSimple(0, key, val, time.Now().Add(ttl).UnixMilli())
}

// SetNX writes only when the key is absent, reporting whether it was written.
func (s *Store) SetNX(key string, val []byte) (bool, error) {
	if err := s.checkAlive(); err != nil {
		return false, err
	}
	_, _, written, err := s.setString(0, key, val, SetOption{NX: true})
	return written, err
}

// GetSet writes a value and returns the previous one.
func (s *Store) GetSet(key string, val []byte) ([]byte, error) {
	if err := s.checkAlive(); err != nil {
		return nil, err
	}
	old, _, _, err := s.setString(0, key, val, SetOption{GET: true})
	return old, err
}

// MGet returns the values of keys, using nil for absent ones.
func (s *Store) MGet(keys ...string) ([][]byte, error) {
	if err := s.checkAlive(); err != nil {
		return nil, err
	}
	out := make([][]byte, len(keys))
	for i, k := range keys {
		val, _, err := s.getString(0, k)
		if err != nil {
			return nil, err
		}
		out[i] = val
	}
	return out, nil
}

// MSet writes several key/value pairs atomically on the durable side.
func (s *Store) MSet(pairs map[string][]byte) error {
	if err := s.checkAlive(); err != nil {
		return err
	}
	args := make([][]byte, 0, len(pairs)*2)
	for k, v := range pairs {
		args = append(args, []byte(k), v)
	}
	return s.mSetRaw(0, args)
}

// Del removes keys and returns how many existed.
func (s *Store) Del(keys ...string) (int64, error) {
	if err := s.checkAlive(); err != nil {
		return 0, err
	}
	return s.delKeys(0, keys)
}

// Exists counts how many of keys are present.
func (s *Store) Exists(keys ...string) (int64, error) {
	if err := s.checkAlive(); err != nil {
		return 0, err
	}
	return s.exists(0, keys)
}

// Expire sets a TTL on a key, reporting whether it was applied.
func (s *Store) Expire(key string, ttl time.Duration) (bool, error) {
	if err := s.checkAlive(); err != nil {
		return false, err
	}
	n, err := s.setExpiry(0, key, time.Now().Add(ttl), ExpireOption{})
	return n == 1, err
}

// TTL returns the remaining time to live. It returns ok=false when the key has
// no TTL, and exists=false when the key is absent.
func (s *Store) TTL(key string) (ttl time.Duration, ok bool, exists bool, err error) {
	if err = s.checkAlive(); err != nil {
		return 0, false, false, err
	}
	ms := s.ttlOf(0, key)
	switch ms {
	case -2:
		return 0, false, false, nil
	case -1:
		return 0, false, true, nil
	}
	return time.Duration(ms) * time.Millisecond, true, true, nil
}

// Persist removes a key's TTL.
func (s *Store) Persist(key string) (bool, error) {
	if err := s.checkAlive(); err != nil {
		return false, err
	}
	return s.persist(0, key)
}

// Incr increments the integer at key and returns the new value.
func (s *Store) Incr(key string) (int64, error) { return s.IncrBy(key, 1) }

// IncrBy adds delta to the integer at key and returns the new value.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	if err := s.checkAlive(); err != nil {
		return 0, err
	}
	cur, typ, expireAt, ok, err := s.getTyped(0, key)
	if err != nil {
		return 0, err
	}
	var n int64
	if ok {
		if typ != config.TypeString {
			return 0, ErrWrongType
		}
		if n, err = toInt64(cur); err != nil {
			return 0, err
		}
	}
	if (delta > 0 && n > maxInt64-delta) || (delta < 0 && n < minInt64-delta) {
		return 0, ErrOverflow
	}
	n += delta
	if err := s.setStringSimple(0, key, []byte(itoa(n)), expireAt); err != nil {
		return 0, err
	}
	return n, nil
}

// Append appends to a string value and returns the new length.
func (s *Store) Append(key string, val []byte) (int64, error) {
	if err := s.checkAlive(); err != nil {
		return 0, err
	}
	cur, typ, expireAt, ok, err := s.getTyped(0, key)
	if err != nil {
		return 0, err
	}
	if ok && typ != config.TypeString {
		return 0, ErrWrongType
	}
	next := make([]byte, 0, len(cur)+len(val))
	next = append(next, cur...)
	next = append(next, val...)
	return int64(len(next)), s.setStringSimple(0, key, next, expireAt)
}

// StrLen returns the length of a string value.
func (s *Store) StrLen(key string) (int64, error) {
	if err := s.checkAlive(); err != nil {
		return 0, err
	}
	val, _, err := s.getString(0, key)
	return int64(len(val)), err
}

// Type returns the Redis type name of a key, or "none".
func (s *Store) Type(key string) (string, error) {
	if err := s.checkAlive(); err != nil {
		return "", err
	}
	typ, ok, err := s.typeOf(0, key)
	if err != nil || !ok {
		if err != nil {
			return "", err
		}
		return "none", nil
	}
	return config.TypeName(typ), nil
}

// Rename moves a key.
func (s *Store) Rename(src, dst string) error {
	if err := s.checkAlive(); err != nil {
		return err
	}
	return s.rename(0, src, dst, false)
}

// FlushDB removes every key in database 0.
func (s *Store) FlushDB() error {
	if err := s.checkAlive(); err != nil {
		return err
	}
	return s.flushDB(0)
}

// DB variants operate on an explicit logical database.

// GetDB is Get on an explicit database.
func (s *Store) GetDB(db uint16, key string) ([]byte, error) {
	if err := s.checkAlive(); err != nil {
		return nil, err
	}
	val, _, err := s.getString(db, key)
	return val, err
}

// SetDB is Set on an explicit database.
func (s *Store) SetDB(db uint16, key string, val []byte) error {
	if err := s.checkAlive(); err != nil {
		return err
	}
	return s.setStringSimple(db, key, val, 0)
}

// DelDB is Del on an explicit database.
func (s *Store) DelDB(db uint16, keys ...string) (int64, error) {
	if err := s.checkAlive(); err != nil {
		return 0, err
	}
	return s.delKeys(db, keys)
}

// delKeys removes several keys.
func (s *Store) delKeys(db uint16, keys []string) (int64, error) {
	var n int64
	for _, k := range keys {
		if _, _, _, ok, err := s.getTyped(db, k); err != nil {
			return 0, err
		} else if !ok {
			continue
		}
		if _, err := s.deleteKey(db, k); err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}

// mSetRaw is the shared MSET implementation for the embedded API.
func (s *Store) mSetRaw(db uint16, args [][]byte) error {
	return s.mSetPairs(db, args)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [24]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
