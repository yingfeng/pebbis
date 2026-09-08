package storage

import (
	"encoding/binary"
	"errors"
	"math"
)

// Encoding of aggregate objects.
//
// Small collections are stored inline as a single value in the data segment,
// the way Redis keeps small hashes in a listpack: one read instead of N, no
// per-element keys, and no read amplification. Once a collection grows past
// InlineMaxEntries it flips to the sparse form, where each element is its own
// key in the type's segment and only a small header stays inline.
//
// Inline layout (per type):
//
//	hash  [EncInline][count:4][flen:2][field][vlen:4][value]...
//	list  [EncInline][count:4][elen:4][element]...
//	set   [EncInline][count:4][mlen:2][member]...
//	zset  [EncInline][count:4][mlen:2][member][score:8]...
//
// Sparse header:
//
//	[EncSparse][count:4][head:8][tail:8]   (head/tail only used by lists)
const (
	EncInline byte = 0
	EncSparse byte = 1
)

// InlineMaxEntries and InlineMaxValueSize bound the inline representation.
const (
	InlineMaxEntries   = 128
	InlineMaxValueSize = 64
)

// ErrCorruptObject is returned when an encoded aggregate fails to decode.
var ErrCorruptObject = errors.New("Pebbis: corrupt aggregate object")

// Member is one scored member of a sorted set.
type Member struct {
	Member string
	Score  float64
}

// Object is the decoded form of an aggregate value.
type Object struct {
	Type byte
	Enc  byte
	// Count is the number of elements.
	Count int
	// Data is the inline payload when Enc == EncInline.
	Data []byte
	// Head/Tail bound the sequence numbers of a sparse list.
	Head, Tail int64
}

// EncodeSparseHeader builds the header kept inline for a sparse collection.
func EncodeSparseHeader(typ byte, count int, head, tail int64) []byte {
	buf := make([]byte, 0, 1+4+16)
	buf = append(buf, EncSparse)
	buf = binary.BigEndian.AppendUint32(buf, uint32(count))
	buf = binary.BigEndian.AppendUint64(buf, uint64(head))
	buf = binary.BigEndian.AppendUint64(buf, uint64(tail))
	return buf
}

// DecodeObjectHeader reads the encoding byte and, for the sparse form, the
// element count and list bounds.
func DecodeObjectHeader(payload []byte) (Object, error) {
	var o Object
	if len(payload) < 1 {
		return o, ErrCorruptObject
	}
	o.Enc = payload[0]
	if o.Enc == EncInline {
		if len(payload) < 5 {
			return o, ErrCorruptObject
		}
		o.Count = int(binary.BigEndian.Uint32(payload[1:5]))
		o.Data = payload[5:]
		return o, nil
	}
	if o.Enc != EncSparse {
		return o, ErrCorruptObject
	}
	if len(payload) < 21 {
		return o, ErrCorruptObject
	}
	o.Count = int(binary.BigEndian.Uint32(payload[1:5]))
	o.Head = int64(binary.BigEndian.Uint64(payload[5:13]))
	o.Tail = int64(binary.BigEndian.Uint64(payload[13:21]))
	return o, nil
}

// ---------- hash ----------

// EncodeHashInline serialises a hash into the inline form.
func EncodeHashInline(fields []string, valueOf func(string) []byte) []byte {
	buf := make([]byte, 0, 5+len(fields)*8)
	buf = append(buf, EncInline)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(fields)))
	for _, f := range fields {
		v := valueOf(f)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(f)))
		buf = append(buf, f...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

// DecodeHashInline reverses EncodeHashInline.
func DecodeHashInline(data []byte) (map[string][]byte, error) {
	out := map[string][]byte{}
	i := 0
	for i < len(data) {
		if i+2 > len(data) {
			return nil, ErrCorruptObject
		}
		flen := int(binary.BigEndian.Uint16(data[i : i+2]))
		i += 2
		if i+flen > len(data) {
			return nil, ErrCorruptObject
		}
		field := string(data[i : i+flen])
		i += flen
		if i+4 > len(data) {
			return nil, ErrCorruptObject
		}
		vlen := int(binary.BigEndian.Uint32(data[i : i+4]))
		i += 4
		if i+vlen > len(data) {
			return nil, ErrCorruptObject
		}
		out[field] = append([]byte(nil), data[i:i+vlen]...)
		i += vlen
	}
	return out, nil
}

// ---------- list ----------

// EncodeListInline serialises a list into the inline form.
func EncodeListInline(elems []string) []byte {
	buf := make([]byte, 0, 5+len(elems)*8)
	buf = append(buf, EncInline)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(elems)))
	for _, e := range elems {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(e)))
		buf = append(buf, e...)
	}
	return buf
}

// DecodeListInline reverses EncodeListInline.
func DecodeListInline(data []byte) ([]string, error) {
	out := []string{}
	i := 0
	for i < len(data) {
		if i+4 > len(data) {
			return nil, ErrCorruptObject
		}
		n := int(binary.BigEndian.Uint32(data[i : i+4]))
		i += 4
		if i+n > len(data) {
			return nil, ErrCorruptObject
		}
		out = append(out, string(data[i:i+n]))
		i += n
	}
	return out, nil
}

// ---------- set ----------

// EncodeSetInline serialises a set into the inline form.
func EncodeSetInline(members []string) []byte {
	buf := make([]byte, 0, 5+len(members)*8)
	buf = append(buf, EncInline)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(members)))
	for _, m := range members {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(m)))
		buf = append(buf, m...)
	}
	return buf
}

// DecodeSetInline reverses EncodeSetInline.
func DecodeSetInline(data []byte) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	i := 0
	for i < len(data) {
		if i+2 > len(data) {
			return nil, ErrCorruptObject
		}
		n := int(binary.BigEndian.Uint16(data[i : i+2]))
		i += 2
		if i+n > len(data) {
			return nil, ErrCorruptObject
		}
		out[string(data[i:i+n])] = struct{}{}
		i += n
	}
	return out, nil
}

// ---------- sorted set ----------

// EncodeZSetInline serialises a sorted set into the inline form.
func EncodeZSetInline(members []Member) []byte {
	buf := make([]byte, 0, 5+len(members)*16)
	buf = append(buf, EncInline)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(members)))
	for _, m := range members {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(m.Member)))
		buf = append(buf, m.Member...)
		buf = binary.BigEndian.AppendUint64(buf, EncodeScore(m.Score))
	}
	return buf
}

// DecodeZSetInline reverses EncodeZSetInline.
func DecodeZSetInline(data []byte) (map[string]float64, error) {
	out := map[string]float64{}
	i := 0
	for i < len(data) {
		if i+2 > len(data) {
			return nil, ErrCorruptObject
		}
		n := int(binary.BigEndian.Uint16(data[i : i+2]))
		i += 2
		if i+n+8 > len(data) {
			return nil, ErrCorruptObject
		}
		member := string(data[i : i+n])
		i += n
		out[member] = DecodeScore(binary.BigEndian.Uint64(data[i : i+8]))
		i += 8
	}
	return out, nil
}

// ---------- sparse key helpers ----------

const (
	SegHash  byte = 0x03
	SegList  byte = 0x04
	SegSet   byte = 0x05
	SegZSetM byte = 0x06
	SegZSetS byte = 0x07
)

// SegmentFor returns the key segment tag of an aggregate type.
func SegmentFor(typ byte) (byte, bool) {
	switch typ {
	case 1: // hash
		return SegHash, true
	case 2: // list
		return SegList, true
	case 3: // set
		return SegSet, true
	case 4: // zset
		return SegZSetM, true
	}
	return 0, false
}

// AppendElemKey writes the per-element key for a sparse collection.
func AppendElemKey(dst []byte, seg byte, db uint16, key, elem string) []byte {
	dst = append(dst, seg)
	dst = binary.BigEndian.AppendUint16(dst, db)
	dst = append(dst, key...)
	dst = append(dst, 0x00) // separator: key may be any bytes
	dst = append(dst, elem...)
	return dst
}

// EncodeElemKey returns the per-element key for a sparse collection.
func EncodeElemKey(seg byte, db uint16, key, elem string) []byte {
	return AppendElemKey(make([]byte, 0, 3+len(key)+len(elem)), seg, db, key, elem)
}

// EncodeListSeqKey builds a sparse list element key from its sequence number.
func EncodeListSeqKey(db uint16, key string, seq int64) []byte {
	buf := make([]byte, 0, 3+len(key)+8)
	buf = append(buf, SegList)
	buf = binary.BigEndian.AppendUint16(buf, db)
	buf = append(buf, key...)
	buf = append(buf, 0x00)
	buf = binary.BigEndian.AppendUint64(buf, uint64(seq))
	return buf
}

// ElemPrefix returns the prefix covering every element of a collection.
func ElemPrefix(seg byte, db uint16, key string) []byte {
	buf := make([]byte, 0, 3+len(key)+1)
	buf = append(buf, seg)
	buf = binary.BigEndian.AppendUint16(buf, db)
	buf = append(buf, key...)
	buf = append(buf, 0x00)
	return buf
}

// ElemFromKey extracts the element part of a per-element key.
func ElemFromKey(prefix, k []byte) string {
	if len(k) < len(prefix) {
		return ""
	}
	return string(k[len(prefix):])
}

// EncodeScoreKey builds the score-index key of a sparse sorted set.
func EncodeScoreKey(db uint16, key string, score float64, member string) []byte {
	buf := make([]byte, 0, 3+8+len(key)+len(member))
	buf = append(buf, SegZSetS)
	buf = binary.BigEndian.AppendUint16(buf, db)
	buf = append(buf, key...)
	buf = append(buf, 0x00)
	buf = binary.BigEndian.AppendUint64(buf, EncodeScore(score))
	buf = append(buf, member...)
	return buf
}

func scorePrefix(db uint16, key string) []byte {
	buf := make([]byte, 0, 3+len(key)+1)
	buf = append(buf, SegZSetS)
	buf = binary.BigEndian.AppendUint16(buf, db)
	buf = append(buf, key...)
	buf = append(buf, 0x00)
	return buf
}

// ScorePrefix exposes the score-index prefix for range scans.
func ScorePrefix(db uint16, key string) []byte { return scorePrefix(db, key) }

func uint64Bytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// ScoreOf decodes the score embedded in a score-index key.
func ScoreOf(prefix, k []byte) float64 {
	if len(k) < len(prefix)+8 {
		return 0
	}
	return DecodeScore(binary.BigEndian.Uint64(k[len(prefix) : len(prefix)+8]))
}

// MemberOf decodes the member embedded in a score-index key.
func MemberOf(prefix, k []byte) string {
	if len(k) < len(prefix)+8 {
		return ""
	}
	return string(k[len(prefix)+8:])
}

// InfScore reports whether f is one of the infinities, which need special
// handling in range commands.
func InfScore(f float64) bool { return math.IsInf(f, 0) }
