// Package storage implements the Pebble-backed persistence layer.
//
// # Keyspace layout
//
// Pebble has no RocksDB-style column families, so a single keyspace is split by
// a one byte tag. Keeping everything in one keyspace is deliberate: multi-write
// commands (RENAME, and later ZADD's double write) must stay atomic, and only a
// single pebble.Batch can guarantee that.
//
//	data   0x02 | db(2B) | key             -> typ(1B) | expireAt(8B) | payload
//	hash   0x03 | db(2B) | key | field     -> value                    (P1)
//	list   0x04 | db(2B) | key | seq(8B)   -> element                  (P1)
//	set    0x05 | db(2B) | key | member    -> empty                    (P1)
//	zsetm  0x06 | db(2B) | key | member    -> score(8B)                (P1)
//	zsets  0x07 | db(2B) | key | score(8B) | member -> empty           (P1)
//	expire 0x08 | expireAt(8B) | db(2B) | key -> empty
//
// The expire segment is ordered by expiry time, so the active expiry cycle is a
// straight prefix scan rather than Redis' random sampling: every round removes
// the keys that are actually due, in due order.
package storage

import (
	"encoding/binary"
	"errors"
	"math"
)

// Key segment tags.
const (
	segData byte = 0x02
	// 0x03 - 0x07 reserved for the aggregate types.
	segExpire byte = 0x08
)

// ValueHeaderSize is the size of the per-value preamble: type tag + expiry.
const ValueHeaderSize = 9

// dbWidth is the fixed width of the logical database id inside a key.
const dbWidth = 2

// ErrCorruptValue is returned when an on-disk value is shorter than its header.
var ErrCorruptValue = errors.New("Pebbis: corrupt value: shorter than header")

// AppendDataKey writes the data key for (db, key) into dst and returns the result.
func AppendDataKey(dst []byte, db uint16, key string) []byte {
	dst = append(dst, segData)
	dst = binary.BigEndian.AppendUint16(dst, db)
	dst = append(dst, key...)
	return dst
}

// EncodeDataKey returns the data key for (db, key).
func EncodeDataKey(db uint16, key string) []byte {
	buf := make([]byte, 0, 1+dbWidth+len(key))
	return AppendDataKey(buf, db, key)
}

// DataPrefix returns the common prefix of every data key in db.
func DataPrefix(db uint16) []byte {
	return AppendDataKey(make([]byte, 0, 1+dbWidth), db, "")
}

// KeyFromDataKey extracts the user key from an encoded data key.
func KeyFromDataKey(dk []byte) string {
	if len(dk) < 1+dbWidth {
		return ""
	}
	return string(dk[1+dbWidth:])
}

// EncodeExpireKey returns the expiry-index key for (expireAtMs, db, key).
func EncodeExpireKey(expireAtMs int64, db uint16, key string) []byte {
	buf := make([]byte, 0, 1+8+dbWidth+len(key))
	return AppendExpireKey(buf, expireAtMs, db, key)
}

// AppendExpireKey writes the expiry-index key into dst and returns the result.
func AppendExpireKey(dst []byte, expireAtMs int64, db uint16, key string) []byte {
	dst = append(dst, segExpire)
	dst = binary.BigEndian.AppendUint64(dst, orderPreserving(expireAtMs))
	dst = binary.BigEndian.AppendUint16(dst, db)
	dst = append(dst, key...)
	return dst
}

// ExpirePrefix returns the common prefix of every expiry-index key.
func ExpirePrefix() []byte {
	return []byte{segExpire}
}

// ExpireUpperBound returns the exclusive upper bound for a prefix scan of the
// expiry index, so iteration stops before the next segment.
func ExpireUpperBound() []byte {
	return []byte{segExpire + 1}
}

// DecodeExpireKey reverses EncodeExpireKey.
func DecodeExpireKey(ek []byte) (expireAtMs int64, db uint16, key string, ok bool) {
	if len(ek) < 1+8+dbWidth || ek[0] != segExpire {
		return 0, 0, "", false
	}
	expireAtMs = unOrderPreserving(binary.BigEndian.Uint64(ek[1 : 1+8]))
	db = binary.BigEndian.Uint16(ek[1+8 : 1+8+dbWidth])
	key = string(ek[1+8+dbWidth:])
	return expireAtMs, db, key, true
}

// AppendValue writes typ, expiry and payload into dst.
func AppendValue(dst []byte, typ byte, expireAtMs int64, payload []byte) []byte {
	dst = append(dst, typ)
	dst = binary.BigEndian.AppendUint64(dst, uint64(expireAtMs))
	dst = append(dst, payload...)
	return dst
}

// EncodeValue returns the encoded on-disk value for a payload.
func EncodeValue(typ byte, expireAtMs int64, payload []byte) []byte {
	buf := make([]byte, 0, ValueHeaderSize+len(payload))
	return AppendValue(buf, typ, expireAtMs, payload)
}

// DecodeValue reverses EncodeValue. The returned payload aliases v.
func DecodeValue(v []byte) (typ byte, expireAtMs int64, payload []byte, err error) {
	if len(v) < ValueHeaderSize {
		return 0, 0, nil, ErrCorruptValue
	}
	typ = v[0]
	expireAtMs = int64(binary.BigEndian.Uint64(v[1:9]))
	payload = v[ValueHeaderSize:]
	return typ, expireAtMs, payload, nil
}

// EncodeScore maps a float64 onto an 8 byte big-endian representation that
// preserves ordering, which is what makes the zsets segment scannable.
func EncodeScore(f float64) uint64 {
	u := math.Float64bits(f)
	if u&(1<<63) != 0 {
		return ^u // negative: flip every bit
	}
	return u ^ (1 << 63) // non-negative: flip the sign bit only
}

// DecodeScore reverses EncodeScore.
func DecodeScore(c uint64) float64 {
	if c&(1<<63) != 0 {
		// Encoded from a non-negative float: only the sign bit was flipped.
		return math.Float64frombits(c ^ (1 << 63))
	}
	// Encoded from a negative float: every bit was flipped.
	return math.Float64frombits(^c)
}

// orderPreserving maps a signed int64 onto an unsigned one so that big-endian
// byte order matches numeric order.
func orderPreserving(v int64) uint64 {
	return uint64(v) ^ (1 << 63)
}

// unOrderPreserving reverses orderPreserving.
func unOrderPreserving(u uint64) int64 {
	return int64(u ^ (1 << 63))
}
