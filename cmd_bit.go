package redistore

import (
	"strconv"

	"github.com/redistore/redistore/config"
)

// Bit commands.
//
// Bit offsets address the string payload in Redis' big-endian bit order:
// offset 0 is the most significant bit of the first byte.

// bitMaxOffset is Redis' cap on a bit offset (512 MB string).
const bitMaxOffset = 4 * 1024 * 1024 * 8

// stringBits fetches the raw string payload of a key, rejecting aggregates.
func (s *Store) stringBits(db uint16, key string) ([]byte, bool, error) {
	payload, typ, _, ok, err := s.getTyped(db, key)
	if err != nil || !ok {
		return nil, false, err
	}
	if typ != config.TypeString {
		return nil, false, ErrWrongType
	}
	return payload, true, nil
}

// bitOf returns the bit at offset in a zero-padded view of data.
func bitOf(data []byte, off int64) int {
	byteIdx := off / 8
	if byteIdx >= int64(len(data)) {
		return 0
	}
	bit := 7 - off%8
	return int(data[byteIdx]>>uint(bit)) & 1
}

func cmdGetBit(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	off, err := bitOffset(args[1])
	if err != nil {
		return err
	}
	data, ok, err := c.Store.stringBits(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok {
		data = nil
	}
	c.writeInt(int64(bitOf(data, off)))
	return nil
}

// bitOffset parses and validates a bit offset argument.
func bitOffset(b []byte) (int64, error) {
	off, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil || off < 0 {
		return 0, ErrNotInteger
	}
	if off >= bitMaxOffset {
		return 0, &protoError{"ERR bit offset is not an integer or out of range"}
	}
	return off, nil
}

func cmdSetBit(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 3); err != nil {
		return err
	}
	off, err := bitOffset(args[1])
	if err != nil {
		return err
	}
	val, err := strconv.ParseInt(string(args[2]), 10, 8)
	if err != nil || (val != 0 && val != 1) {
		return &protoError{"ERR bit is not an integer or out of range"}
	}
	data, ok, err := c.Store.stringBits(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok {
		data = nil
	}
	prev := bitOf(data, off)
	byteIdx := off / 8
	if byteIdx >= int64(len(data)) {
		grown := make([]byte, byteIdx+1)
		copy(grown, data)
		data = grown
	}
	bit := uint(7 - off%8)
	if val == 1 {
		data[byteIdx] |= 1 << bit
	} else {
		data[byteIdx] &^= 1 << bit
	}
	if err := c.Store.setStringSimple(c.DB, string(args[0]), data, 0); err != nil {
		return err
	}
	c.writeInt(int64(prev))
	return nil
}

// byteRange normalises a BITCOUNT/BITPOS byte range: negative indices count
// from the end and the result is clamped to the string. ok is false for an
// empty range.
func byteRange(data []byte, start, end int64) (lo, hi int64, ok bool) {
	if start < 0 {
		start += int64(len(data))
	}
	if end < 0 {
		end += int64(len(data))
	}
	if start < 0 {
		start = 0
	}
	// Redis clamps a negative end (after normalisation) to byte 0 rather
	// than treating it as an inverted range (BITPOS a 0 0 -999 → 0).
	if end < 0 {
		end = 0
	}
	if end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	if start > end || start >= int64(len(data)) {
		return 0, 0, false
	}
	return start, end, true
}

func cmdBitCount(c *Ctx, args [][]byte) error {
	// Accepted forms: key, key start end, key start end BYTE|BIT.
	switch len(args) {
	case 0:
		return WrongArgs("bitcount")
	case 1, 3, 4:
	default:
		return ErrSyntax
	}
	if len(args) == 4 && !foldEqual(args[3], "BYTE") && !foldEqual(args[3], "BIT") {
		return ErrSyntax
	}
	data, ok, err := c.Store.stringBits(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok || len(data) == 0 {
		c.writeInt(0)
		return nil
	}
	lo, hi := int64(0), int64(len(data))-1
	if len(args) >= 3 {
		start, err := toInt64(args[1])
		if err != nil {
			return err
		}
		end, err := toInt64(args[2])
		if err != nil {
			return err
		}
		if !ok {
			data = nil
		}
		lo, hi, ok = byteRange(data, start, end)
		if !ok {
			c.writeInt(0)
			return nil
		}
	} else if !ok {
		data = nil
	}
	var n int64
	for _, b := range data[lo : hi+1] {
		n += int64(bitPopCount(b))
	}
	c.writeInt(n)
	return nil
}

func bitPopCount(b byte) int {
	n := 0
	for ; b != 0; b &= b - 1 {
		n++
	}
	return n
}

func cmdBitPos(c *Ctx, args [][]byte) error {
	if len(args) < 2 || len(args) > 4 {
		return WrongArgs("bitpos")
	}
	want, err := strconv.ParseInt(string(args[1]), 10, 8)
	if err != nil || (want != 0 && want != 1) {
		return ErrNotInteger
	}
	data, ok, err := c.Store.stringBits(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok {
		// Redis: a missing key is an infinite array of zero bits — the
		// answer does not depend on the range arguments.
		if want == 0 {
			c.writeInt(0)
		} else {
			c.writeInt(-1)
		}
		return nil
	}
	if len(data) == 0 {
		// An existing empty string: after normalisation start (0) is always
		// past the last byte (-1), so the range is empty and contains no bit.
		c.writeInt(-1)
		return nil
	}
	hasEnd := len(args) >= 4
	lo, hi := int64(0), int64(len(data))-1
	// Docs: the right of the string is treated as zero-padded when looking
	// for clear bits **only if no range or just start is given**; with both
	// start and end, a missing clear bit yields -1.
	if len(args) >= 3 {
		start, err := toInt64(args[2])
		if err != nil {
			return err
		}
		end := int64(len(data)) - 1
		if hasEnd {
			var err error
			end, err = toInt64(args[3])
			if err != nil {
				return err
			}
		}
		lo, hi, ok = byteRange(data, start, end)
		if !ok {
			// An inverted/empty range contains no bits at all.
			c.writeInt(-1)
			return nil
		}
	}
	wholeString := !hasEnd && hi == int64(len(data))-1
	for i := lo; i <= hi; i++ {
		b := data[i]
		// Skip bytes that cannot contain the wanted bit.
		if (want == 0 && b == 0xff) || (want == 1 && b == 0) {
			continue
		}
		for bit := 0; bit < 8; bit++ {
			if int64(b>>uint(7-bit))&1 == want {
				c.writeInt(i*8 + int64(bit))
				return nil
			}
		}
	}
	// Searching for a zero bit in a range that reaches the end of the string
	// treats the (missing) trailing bytes as implicit zeros.
	if want == 0 && wholeString {
		c.writeInt(int64(len(data)) * 8)
		return nil
	}
	c.writeInt(-1)
	return nil
}

func cmdBitOp(c *Ctx, args [][]byte) error {
	if len(args) < 3 {
		return WrongArgs("bitop")
	}
	op := string(args[0])
	dst := string(args[1])
	srcs := args[2:]

	var apply func(a, b byte) byte
	switch op {
	case "AND", "OR", "XOR":
		if len(srcs) == 0 {
			return WrongArgs("bitop")
		}
		switch op {
		case "AND":
			apply = func(a, b byte) byte { return a & b }
		case "OR":
			apply = func(a, b byte) byte { return a | b }
		case "XOR":
			apply = func(a, b byte) byte { return a ^ b }
		}
	case "NOT":
		if len(srcs) != 1 {
			return &protoError{"ERR BITOP NOT must be called with a single source key."}
		}
	default:
		return WrongArgs("bitop")
	}

	var res []byte
	if op == "NOT" {
		data, ok, err := c.Store.stringBits(c.DB, string(srcs[0]))
		if err != nil {
			return err
		}
		if !ok {
			data = nil
		}
		res = make([]byte, len(data))
		for i, b := range data {
			res[i] = ^b
		}
	} else {
		for _, src := range srcs {
			data, ok, err := c.Store.stringBits(c.DB, string(src))
			if err != nil {
				return err
			}
			if !ok {
				data = nil
			}
			if res == nil {
				res = append([]byte{}, data...)
				continue
			}
			if len(data) > len(res) {
				grown := make([]byte, len(data))
				copy(grown, res)
				res = grown
			}
			for i := range res {
				var b byte
				if i < len(data) {
					b = data[i]
				}
				res[i] = apply(res[i], b)
			}
		}
	}
	if res == nil {
		res = []byte{}
	}
	if len(res) == 0 {
		// Redis deletes the destination when the result is empty.
		if _, err := c.Store.deleteKey(c.DB, dst); err != nil {
			return err
		}
		c.writeInt(0)
		return nil
	}
	if err := c.Store.setStringSimple(c.DB, dst, res, 0); err != nil {
		return err
	}
	c.writeInt(int64(len(res)))
	return nil
}
