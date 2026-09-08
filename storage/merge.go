package storage

import (
	"encoding/binary"
	"io"
	"math"
	"math/big"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/pebbis/pebbis/config"
)

// Merge operator tags. A merge operand is [tag(1)][payload]. Integers carry an
// 8-byte big-endian delta; floats carry the decimal spelling of the delta so the
// operator reproduces the exact big.Float arithmetic of the command layer.
const (
	mergeTagInt   byte = 0x01
	mergeTagFloat byte = 0x02
)

// EncodeIntDelta builds the merge operand for INCR/DECR/INCRBY/DECRBY. The whole
// signed delta fits in one operand; the merge operator folds every operand for a
// key, so concurrent increments on the same key each contribute their own delta
// and none are lost.
func EncodeIntDelta(d int64) []byte {
	b := make([]byte, 1+8)
	b[0] = mergeTagInt
	binary.BigEndian.PutUint64(b[1:], uint64(d))
	return b
}

// EncodeFloatDelta builds the merge operand for INCRBYFLOAT. The delta is stored
// as its decimal string (not float64 bits) so the operator re-parses it with the
// same 128-bit precision the command layer used, producing identical results.
func EncodeFloatDelta(s string) []byte {
	b := make([]byte, 1+len(s))
	b[0] = mergeTagFloat
	copy(b[1:], s)
	return b
}

// counterMerge implements pebble's merge operator for the commutative counter
// class (INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT). It is associative and
// commutative: an arbitrary number of deltas applied in any order to any base
// value yields the same result, which is exactly what makes the increment
// lock-free - Pebble applies the operator at read/compaction time while writers
// simply append operands, never serialising on a mutex.
type counterMerge struct{}

// Merge is called with the current on-disk base value for a key and returns a
// ValueMerger that folds the pending operands into it. The base may be:
//   - nil/empty: the key is absent, so we start from zero;
//   - a stored value encoded by EncodeValue(typ|expireAt|payload) with a string
//     type: the resting counter value;
//   - a merge operand (starts with mergeTagInt/mergeTagFloat): when a key has only
//     ever been MERGEd (no Put/Delete base), Pebble passes the first operand as
//     the "base"; we seed zero and fold that operand in.
func (counterMerge) Merge(key, value []byte) (pebble.ValueMerger, error) {
	m := &counterValueMerger{cur: new(big.Float).SetPrec(128), base: value}

	switch {
	case len(value) == 0:
		// Key absent: an increment from zero.
		m.cur.SetInt64(0)
		return m, nil

	case value[0] == mergeTagInt || value[0] == mergeTagFloat:
		// First delta delivered as the "base" (key has only ever been merged).
		m.cur.SetInt64(0)
		_ = m.fold(value)
		return m, nil

	case value[0] == config.TypeString:
		typ, expireAt, payload, err := DecodeValue(value)
		if err != nil || typ != config.TypeString {
			// Unreadable or non-string base: never corrupt it.
			return &passthroughMerger{base: value}, nil
		}
		m.expireAt = expireAt
		if len(payload) == 0 {
			m.cur.SetInt64(0)
		} else if _, ok := m.cur.SetString(string(payload)); !ok {
			// Current value is not a number: applying a delta would be undefined, so
			// drop the operands and keep the value untouched.
			return &passthroughMerger{base: value}, nil
		}
		return m, nil

	default:
		// Non-string base (list/hash/zset/...): never fold onto it.
		return &passthroughMerger{base: value}, nil
	}
}

// counterValueMerger folds merge operands into the base value. Because addition
// is commutative, MergeNewer and MergeOlder do the same thing.
type counterValueMerger struct {
	expireAt int64
	cur      *big.Float
	base     []byte // original value, returned unchanged if a fold is unsafe
}

func (m *counterValueMerger) MergeNewer(value []byte) error { return m.fold(value) }

func (m *counterValueMerger) MergeOlder(value []byte) error { return m.fold(value) }

func (m *counterValueMerger) fold(op []byte) error {
	if len(op) == 0 {
		// A delete tombstone (or empty base) contributes nothing: the key is
		// absent, so the counter starts from its accumulated tail.
		return nil
	}
	switch op[0] {
	case mergeTagInt:
		if len(op) < 9 {
			return nil
		}
		d := int64(binary.BigEndian.Uint64(op[1:9]))
		m.cur.Add(m.cur, new(big.Float).SetPrec(128).SetInt64(d))
	case mergeTagFloat:
		df, ok := new(big.Float).SetPrec(128).SetString(string(op[1:]))
		if !ok {
			return nil
		}
		m.cur.Add(m.cur, df)
	case config.TypeString:
		// Pebble folds oldest-first: when the newest entry of a key is a merge
		// operand, an older Put (the resting counter value) arrives here as a
		// "mergeOlder" operand. It is an absolute base, not a delta, so we ADD
		// its numeric value (addition is commutative, so the order is irrelevant)
		// and inherit its expiry.
		typ, expireAt, payload, err := DecodeValue(op)
		if err != nil || typ != config.TypeString {
			return nil
		}
		if expireAt > m.expireAt {
			m.expireAt = expireAt
		}
		if len(payload) == 0 {
			return nil
		}
		bf, ok := new(big.Float).SetPrec(128).SetString(string(payload))
		if !ok {
			// Non-numeric base: keep the merged operands, ignore the bad base.
			return nil
		}
		m.cur.Add(m.cur, bf)
	}
	return nil
}

func (m *counterValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	if m.cur.IsInf() {
		// Should not happen - the command layer rejects NaN/Inf deltas - but if a
		// stack of deltas ever produced it, keep the base rather than persist a
		// non-finite value.
		return m.base, nil, nil
	}
	return EncodeValue(config.TypeString, m.expireAt, []byte(formatCounterFloat(m.cur))), nil, nil
}

// passthroughMerger yields the base value unchanged and ignores all operands. It
// is used when a key that is not a numeric counter is (erroneously) merged onto,
// so the original value is never corrupted.
type passthroughMerger struct{ base []byte }

func (m *passthroughMerger) MergeNewer(value []byte) error { return nil }
func (m *passthroughMerger) MergeOlder(value []byte) error { return nil }
func (m *passthroughMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return m.base, nil, nil
}

// formatCounterFloat renders a counter value the way Redis's ld2string does:
// fixed notation with trailing zeros trimmed, integers without a decimal point.
// It is identical to formatPrecFloat in the command layer so the merged result
// matches the read-modify-write path byte for byte.
func formatCounterFloat(v *big.Float) string {
	if v.IsInf() {
		return "inf"
	}
	if f, _ := v.Float64(); math.Abs(f-math.Round(f)) < 1e-9 {
		// big.Float keeps a sign on zero, so an add-then-subtract landing on -0
		// would otherwise format as "-0". Fold IEEE -0.0 into +0.0.
		v = big.NewFloat(math.Round(f) + 0.0)
	}
	s := v.Text('f', 17)
	if i := strings.IndexByte(s, '.'); i >= 0 {
		tail := strings.TrimRight(s[i+1:], "0")
		if tail == "" {
			s = s[:i]
		} else {
			s = s[:i+1] + tail
		}
	}
	return s
}
