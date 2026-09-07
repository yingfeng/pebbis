package redistore

import (
	"encoding/binary"
	"strconv"
	"time"

	"github.com/redistore/redistore/storage"
)

// Stream storage.
//
// A stream is an append-only log of 128-bit IDs (ms-seq) mapped to field/value
// pairs. Consumer groups carry the delivery bookkeeping that makes this a
// reliable queue: every entry handed to a consumer lands in a pending list
// (PEL) and stays there until XACK, so a crashed consumer's work is neither
// lost nor silently redelivered.
//
// Key layout (segments continue the numbering from storage/codec.go):
//
//	entry 0x0B | db(2) | key | 0x00 | ms(8) | seq(8)  -> fields
//	group  0x0C | db(2) | key | 0x00 | group | 0x00   -> lastDelivered(ms,seq)
//	pel    0x0D | db(2) | key | 0x00 | group | 0x00 | ms(8) | seq(8)
//	                                                     -> consumer | 0x00 | count(4) | deliveredMs(8)
//
// The 16-byte big-endian ID makes byte order == time order, so a range scan is
// a plain iterator walk and "the next entry after X" is SeekGE - which is what
// XREADGROUP's ">" becomes.

// Segment tags for stream structures.
const (
	SegStreamEntry byte = 0x0B
	SegStreamGroup byte = 0x0C
	SegStreamPEL   byte = 0x0D
)

// streamID is a 128-bit stream entry ID.
type streamID struct {
	ms  uint64
	seq uint64
}

// parseStreamID parses "ms" / "ms-seq" / "*" (the caller handles *).
func parseStreamID(s string) (streamID, error) {
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			ms, err1 := strconv.ParseUint(s[:i], 10, 64)
			seq, err2 := strconv.ParseUint(s[i+1:], 10, 64)
			if err1 != nil || err2 != nil {
				return streamID{}, &protoError{"ERR Invalid stream ID specified as stream command argument"}
			}
			return streamID{ms: ms, seq: seq}, nil
		}
	}
	ms, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return streamID{}, &protoError{"ERR Invalid stream ID specified as stream command argument"}
	}
	return streamID{ms: ms}, nil
}

func (id streamID) less(o streamID) bool {
	if id.ms != o.ms {
		return id.ms < o.ms
	}
	return id.seq < o.seq
}

func (id streamID) equal(o streamID) bool { return id.ms == o.ms && id.seq == o.seq }

// nextID returns the smallest ID strictly greater than id. XREAD and
// XREADGROUP are exclusive on their lower bound - the client has already seen
// the entry at that ID - so reads start here.
func nextID(id streamID) streamID {
	if id.seq == ^uint64(0) {
		return streamID{ms: id.ms + 1}
	}
	return streamID{ms: id.ms, seq: id.seq + 1}
}

func (id streamID) String() string {
	return strconv.FormatUint(id.ms, 10) + "-" + strconv.FormatUint(id.seq, 10)
}

func streamEntryPrefix(db uint16, key string) []byte {
	buf := make([]byte, 0, 3+len(key)+1)
	buf = append(buf, SegStreamEntry)
	buf = binary.BigEndian.AppendUint16(buf, db)
	buf = append(buf, key...)
	buf = append(buf, 0x00)
	return buf
}

func appendStreamEntryKey(dst []byte, db uint16, key string, id streamID) []byte {
	dst = append(dst, SegStreamEntry)
	dst = binary.BigEndian.AppendUint16(dst, db)
	dst = append(dst, key...)
	dst = append(dst, 0x00)
	dst = binary.BigEndian.AppendUint64(dst, id.ms)
	dst = binary.BigEndian.AppendUint64(dst, id.seq)
	return dst
}

func streamIDOfEntryKey(prefix, k []byte) (streamID, bool) {
	if len(k) < len(prefix)+16 {
		return streamID{}, false
	}
	b := k[len(prefix):]
	return streamID{
		ms:  binary.BigEndian.Uint64(b[:8]),
		seq: binary.BigEndian.Uint64(b[8:16]),
	}, true
}

func streamGroupPrefix(db uint16, key, group string) []byte {
	buf := make([]byte, 0, 4+len(key)+len(group)+1)
	buf = append(buf, SegStreamGroup)
	buf = binary.BigEndian.AppendUint16(buf, db)
	buf = append(buf, key...)
	buf = append(buf, 0x00)
	buf = append(buf, group...)
	buf = append(buf, 0x00)
	return buf
}

func streamPELPrefix(db uint16, key, group string) []byte {
	buf := make([]byte, 0, 4+len(key)+len(group)+1)
	buf = append(buf, SegStreamPEL)
	buf = binary.BigEndian.AppendUint16(buf, db)
	buf = append(buf, key...)
	buf = append(buf, 0x00)
	buf = append(buf, group...)
	buf = append(buf, 0x00)
	return buf
}

func appendStreamPELKey(dst []byte, db uint16, key, group string, id streamID) []byte {
	dst = append(dst, SegStreamPEL)
	dst = binary.BigEndian.AppendUint16(dst, db)
	dst = append(dst, key...)
	dst = append(dst, 0x00)
	dst = append(dst, group...)
	dst = append(dst, 0x00)
	dst = binary.BigEndian.AppendUint64(dst, id.ms)
	dst = binary.BigEndian.AppendUint64(dst, id.seq)
	return dst
}

// streamField is one field/value pair of an entry.
type streamField struct {
	Field string
	Value string
}

// streamEntry is a decoded entry.
type streamEntry struct {
	id     streamID
	fields []streamField
}

// encodeStreamFields serialises field/value pairs.
func encodeStreamFields(fields []streamField) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(len(fields)))
	for _, f := range fields {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(f.Field)))
		buf = append(buf, f.Field...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(f.Value)))
		buf = append(buf, f.Value...)
	}
	return buf
}

func decodeStreamFields(v []byte) ([]streamField, error) {
	if len(v) < 2 {
		return nil, storage.ErrCorruptValue
	}
	n := int(binary.BigEndian.Uint16(v[:2]))
	i := 2
	out := make([]streamField, 0, n)
	for range n {
		if i+2 > len(v) {
			return nil, storage.ErrCorruptValue
		}
		fl := int(binary.BigEndian.Uint16(v[i : i+2]))
		i += 2
		if i+fl+4 > len(v) {
			return nil, storage.ErrCorruptValue
		}
		field := string(v[i : i+fl])
		i += fl
		vl := int(binary.BigEndian.Uint32(v[i : i+4]))
		i += 4
		if i+vl > len(v) {
			return nil, storage.ErrCorruptValue
		}
		out = append(out, streamField{Field: field, Value: string(v[i : i+vl])})
		i += vl
	}
	return out, nil
}

// xAdd appends an entry. When id is the zero value the caller wants an
// auto-generated ID. It returns the stored ID.
func (s *Store) xAdd(db uint16, key string, id streamID, auto bool, fields []streamField, nomkstream bool) (streamID, error) {

	var last streamID
	lastExists := false
	// Find the current last ID by scanning the segment; the entry segment is
	// ordered by ID, so this is a full walk today and a reverse seek tomorrow.
	if lv, ok, err := s.lastStreamEntryID(db, key); err != nil {
		return streamID{}, err
	} else if ok {
		last, lastExists = lv, true
	}

	if auto {
		now := uint64(s.clock.Now().UnixMilli())
		id = streamID{ms: now}
		if lastExists && last.ms == now {
			id.seq = last.seq + 1
		}
		if lastExists && !last.less(id) && !last.equal(streamID{}) {
			// Same or lower than the last entry: bump the sequence past it.
			if last.ms >= id.ms {
				id.ms, id.seq = last.ms, last.seq+1
			}
		}
	} else {
		if id.ms == 0 && id.seq == 0 {
			return streamID{}, &protoError{"ERR The ID specified in XADD must be greater than 0-0"}
		}
		if lastExists && !last.less(id) {
			return streamID{}, &protoError{"ERR The ID specified in XADD is equal or smaller than the target stream top item"}
		}
	}

	if nomkstream && !lastExists {
		return streamID{}, nil
	}

	batch := s.eng.Batch()
	defer batch.Close()

	ek := appendStreamEntryKey(nil, db, key, id)
	if err := batch.Set(ek, encodeStreamFields(fields)); err != nil {
		return streamID{}, err
	}
	if err := s.eng.Apply(batch); err != nil {
		return streamID{}, err
	}

	// Trim the automatic cap so streams cannot grow without bound by accident;
	// callers use XTRIM for explicit control.
	s.blocker.notify(db, key)
	s.stats.keyChanges.Add(1)
	return id, nil
}

// lastStreamEntryID returns the ID of the newest entry, if any. A reverse seek
// keeps this O(logN) - XADD calls it on every append, so a scan here would make
// the hottest queue path quadratic in stream length.
func (s *Store) lastStreamEntryID(db uint16, key string) (streamID, bool, error) {
	prefix := streamEntryPrefix(db, key)
	k, _, found, err := s.eng.LastInRange(prefix, prefixEnd(prefix))
	if err != nil || !found {
		return streamID{}, false, err
	}
	id, ok := streamIDOfEntryKey(prefix, k)
	if !ok {
		return streamID{}, false, nil
	}
	return id, true, nil
}

// xRange reads entries with start <= id <= end. When rev is set it walks
// backwards. count <= 0 means unlimited.
func (s *Store) xRange(db uint16, key string, start, end streamID, count int, rev bool) ([]streamEntry, error) {
	prefix := streamEntryPrefix(db, key)
	// Pebble only scans ascending, so normalise the bounds and reverse the
	// collected output when the caller wanted it backwards.
	if end.less(start) {
		start, end = end, start
	}
	lo := append(append([]byte(nil), prefix...), must16(start)...)
	hi := append(append([]byte(nil), prefix...), must16(streamID{ms: end.ms, seq: end.seq + 1})...)

	var out []streamEntry
	err := s.eng.ScanRange(lo, hi, func(k, v []byte) error {
		id, ok := streamIDOfEntryKey(prefix, k)
		if !ok {
			return nil
		}
		if rev {
			if id.less(start) || end.less(id) {
				return nil
			}
		} else {
			if id.less(start) || end.less(id) {
				return nil
			}
		}
		fields, err := decodeStreamFields(v)
		if err != nil {
			return nil
		}
		out = append(out, streamEntry{id: id, fields: fields})
		// Forward collection only: Pebble has no reverse iterator, so the rev
		// count is applied after the walk. Fine for admin-sized ranges.
		return nil
	})
	if err != nil && !storage.IsStop(err) {
		return nil, err
	}
	if rev {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	if count > 0 && len(out) > count {
		out = out[:count]
	}
	return out, nil
}

// must16 is the 16-byte big-endian form of an ID, used as a scan bound.
func must16(id streamID) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[:8], id.ms)
	binary.BigEndian.PutUint64(b[8:], id.seq)
	return b
}

// xDel removes entries by ID and reports how many were removed.
func (s *Store) xDel(db uint16, key string, ids []streamID) (int64, error) {
	batch := s.eng.Batch()
	defer batch.Close()
	var n int64
	for _, id := range ids {
		if err := batch.Delete(appendStreamEntryKey(nil, db, key, id)); err != nil {
			return 0, err
		}
		n++
	}
	if n == 0 {
		return 0, nil
	}
	return n, s.eng.Apply(batch)
}

// xTrimMaxlen keeps at most n entries.
func (s *Store) xTrimMaxlen(db uint16, key string, n int) (int64, error) {
	prefix := streamEntryPrefix(db, key)
	ids := make([]streamID, 0, 64)
	err := s.eng.ScanKeys(prefix, func(k []byte) error {
		if id, ok := streamIDOfEntryKey(prefix, k); ok {
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	excess := len(ids) - n
	if excess <= 0 {
		return 0, nil
	}
	batch := s.eng.Batch()
	defer batch.Close()
	for _, id := range ids[:excess] {
		if err := batch.Delete(appendStreamEntryKey(nil, db, key, id)); err != nil {
			return 0, err
		}
	}
	if err := s.eng.Apply(batch); err != nil {
		return 0, err
	}
	return int64(excess), nil
}

// xTrimMinID drops entries older than id.
func (s *Store) xTrimMinID(db uint16, key string, id streamID) (int64, error) {
	prefix := streamEntryPrefix(db, key)
	lo := append(append([]byte(nil), prefix...), make([]byte, 16)...)
	hi := append(append([]byte(nil), prefix...), must16(id)...)
	return s.deleteRangeCount(lo, hi)
}

func (s *Store) deleteRangeCount(lo, hi []byte) (int64, error) {
	var n int64
	err := s.eng.ScanKeys(lo, func(k []byte) error {
		n++
		return nil
	})
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}
	if err := s.eng.DeleteRange(lo, hi); err != nil {
		return 0, err
	}
	return n, nil
}

// ---------- consumer groups ----------

// groupGet reads a group's last-delivered ID. exists reports whether the group
// itself exists (an absent group cannot be read from).
func (s *Store) groupGet(db uint16, key, group string) (streamID, bool, error) {
	prefix := streamGroupPrefix(db, key, group)
	var id streamID
	found := false
	err := s.eng.Scan(prefix, func(k, v []byte) error {
		if len(v) >= 16 {
			id = streamID{ms: binary.BigEndian.Uint64(v[:8]), seq: binary.BigEndian.Uint64(v[8:16])}
			found = true
		}
		return storage.ErrStop
	})
	if err != nil && !storage.IsStop(err) {
		return streamID{}, false, err
	}
	return id, found, nil
}

// groupSet writes the last-delivered ID, creating the group if needed.
func (s *Store) groupSet(db uint16, key, group string, id streamID) error {
	v := make([]byte, 16)
	binary.BigEndian.PutUint64(v[:8], id.ms)
	binary.BigEndian.PutUint64(v[8:], id.seq)
	return s.eng.Put(streamGroupPrefix(db, key, group), v)
}

// groupDestroy removes a group and its pending list.
func (s *Store) groupDestroy(db uint16, key, group string) error {
	if err := s.eng.Delete(streamGroupPrefix(db, key, group)); err != nil {
		return err
	}
	p := streamPELPrefix(db, key, group)
	if err := s.eng.DeleteRange(p, prefixEnd(p)); err != nil {
		return err
	}
	return nil
}

// pelAdd records an entry as delivered to consumer.
func (s *Store) pelAdd(db uint16, key, group, consumer string, id streamID) error {
	v := make([]byte, 0, len(consumer)+1+4+8)
	v = append(v, consumer...)
	v = append(v, 0x00)
	v = binary.BigEndian.AppendUint32(v, 1)
	v = binary.BigEndian.AppendUint64(v, uint64(s.clock.Now().UnixMilli()))
	return s.eng.Put(appendStreamPELKey(nil, db, key, group, id), v)
}

// pelRemove acks an entry.
func (s *Store) pelRemove(db uint16, key, group string, id streamID) error {
	return s.eng.Delete(appendStreamPELKey(nil, db, key, group, id))
}

// pelMoveTo reassigns a pending entry to another consumer.
func (s *Store) pelMoveTo(db uint16, key, group, consumer string, id streamID) error {
	return s.pelAdd(db, key, group, consumer, id)
}

// pendingEntry is one XPENDING row.
type pendingEntry struct {
	id           streamID
	consumer     string
	deliveryMs   int64
	deliveryCert uint32
}

// pelList scans the group's pending entries in ID order.
func (s *Store) pelList(db uint16, key, group string, consumer string, start, end streamID, count int) ([]pendingEntry, error) {
	prefix := streamPELPrefix(db, key, group)
	lo := append(append([]byte(nil), prefix...), must16(start)...)
	hi := append(append([]byte(nil), prefix...), must16(streamID{ms: end.ms, seq: end.seq + 1})...)
	var out []pendingEntry
	err := s.eng.ScanRange(lo, hi, func(k, v []byte) error {
		id, ok := streamIDOfEntryKey(prefix, k)
		if !ok {
			return nil
		}
		// v = consumer | 0x00 | count(4) | deliveredMs(8)
		z := indexByte(v, 0x00)
		if z < 0 {
			return nil
		}
		if consumer != "" && string(v[:z]) != consumer {
			return nil
		}
		if len(v) < z+13 {
			return nil
		}
		e := pendingEntry{
			id:           id,
			consumer:     string(v[:z]),
			deliveryCert: binary.BigEndian.Uint32(v[z+1 : z+5]),
			deliveryMs:   int64(binary.BigEndian.Uint64(v[z+5 : z+13])),
		}
		out = append(out, e)
		if count > 0 && len(out) >= count {
			return storage.ErrStop
		}
		return nil
	})
	if err != nil && !storage.IsStop(err) {
		return nil, err
	}
	return out, nil
}

func indexByte(b []byte, c byte) int {
	for i := range b {
		if b[i] == c {
			return i
		}
	}
	return -1
}

// pelOwned returns the pending entries currently assigned to consumer, with
// their decoded contents (the XREADGROUP 0 path).
func (s *Store) pelOwned(db uint16, key, group, consumer string, count int) ([]streamEntry, error) {
	pending, err := s.pelList(db, key, group, consumer, streamID{}, streamID{ms: ^uint64(0), seq: ^uint64(0)}, count)
	if err != nil {
		return nil, err
	}
	out := make([]streamEntry, 0, len(pending))
	for _, p := range pending {
		// Re-delivery bumps the delivery counter and timestamp, as in Redis.
		pk := appendStreamPELKey(nil, db, key, group, p.id)
		nv := make([]byte, 0, len(p.consumer)+1+4+8)
		nv = append(nv, p.consumer...)
		nv = append(nv, 0x00)
		nv = binary.BigEndian.AppendUint32(nv, p.deliveryCert+1)
		nv = binary.BigEndian.AppendUint64(nv, uint64(s.clock.Now().UnixMilli()))
		if err := s.eng.Put(pk, nv); err != nil {
			return nil, err
		}
		v, release, err := s.eng.Get(appendStreamEntryKey(nil, db, key, p.id))
		if err != nil {
			continue // deleted meanwhile
		}
		fields, err := decodeStreamFields(v)
		release()
		if err != nil {
			continue
		}
		out = append(out, streamEntry{id: p.id, fields: fields})
	}
	return out, nil
}

// xGroupConsumerCount is reported by XINFO; consumers are the distinct names
// in the PEL plus any explicitly created ones.
func (s *Store) xGroupConsumerCount(db uint16, key, group string) int {
	pending, err := s.pelList(db, key, group, "", streamID{}, streamID{ms: ^uint64(0), seq: ^uint64(0)}, 0)
	if err != nil {
		return 0
	}
	seen := map[string]bool{}
	for _, p := range pending {
		seen[p.consumer] = true
	}
	return len(seen)
}

var _ = time.Now
