package redistore

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/glob"
	"github.com/redistore/redistore/storage"
)

// Generic (key-space) commands.

// cmdSort implements the core of SORT: ordering the members of a list, set or
// zset numerically (lexicographically with ALPHA), with LIMIT and ASC/DESC,
// plus the STORE variant. BY/GET patterns are not supported.
func cmdSort(c *Ctx, args [][]byte) error {
	if len(args) < 1 {
		return WrongArgs("sort")
	}
	key := string(args[0])
	limitOff, limitCount := 0, -1
	alpha, desc := false, false
	storeDst := ""
	byPattern := ""
	hasBy := false
	var getPatterns []string
	for i := 1; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "ASC":
			desc = false
		case "DESC":
			desc = true
		case "ALPHA":
			alpha = true
		case "LIMIT":
			if i+2 >= len(args) {
				return ErrSyntax
			}
			off, err1 := atoi(args[i+1])
			cnt, err2 := atoi(args[i+2])
			if err1 != nil || err2 != nil || off < 0 {
				return ErrSyntax
			}
			limitOff, limitCount = off, cnt
			i += 2
		case "BY":
			if i+1 >= len(args) {
				return ErrSyntax
			}
			byPattern = string(args[i+1])
			hasBy = true
			i++
		case "GET":
			if i+1 >= len(args) {
				return ErrSyntax
			}
			getPatterns = append(getPatterns, string(args[i+1]))
			i++
		case "STORE":
			if i+1 >= len(args) {
				return ErrSyntax
			}
			storeDst = string(args[i+1])
			i++
		default:
			return ErrSyntax
		}
	}

	// Collect the source members. A missing key sorts the empty list.
	_, typ, _, ok, err := c.Store.getTyped(c.DB, key)
	if err != nil {
		return err
	}
	var items []string
	if ok {
		switch typ {
		case config.TypeList:
			a, err := c.Store.loadAgg(c.DB, key, config.TypeList)
			if err != nil {
				return err
			}
			items = a.list
		case config.TypeSet:
			a, err := c.Store.loadAgg(c.DB, key, config.TypeSet)
			if err != nil {
				return err
			}
			for m := range a.set {
				items = append(items, m)
			}
		case config.TypeZSet:
			a, err := c.Store.loadAgg(c.DB, key, config.TypeZSet)
			if err != nil {
				return err
			}
			for m := range a.zset {
				items = append(items, m)
			}
			// A zset's members are ordered by score (then lexicographically);
			// that is the order SORT starts from, and what "BY nosort" keeps.
			sort.Slice(items, func(i, j int) bool {
				if a.zset[items[i]] != a.zset[items[j]] {
					return a.zset[items[i]] < a.zset[items[j]]
				}
				return items[i] < items[j]
			})
		default:
			return ErrWrongType
		}
	}

	// Resolve the sort weights. "BY nosort" (or a pattern with no *) keeps the
	// source order; otherwise each element is looked up through the pattern.
	// Only an explicit "BY nosort" skips sorting; a constant pattern (no "*")
	// simply gives every element the same weight, which the tie-break then
	// orders by element.
	nosort := hasBy && strings.EqualFold(byPattern, "nosort")

	type sortRow struct {
		val string
		num float64
		str string
	}
	rows := make([]sortRow, 0, len(items))
	if hasBy && !nosort {
		for _, it := range items {
			w, err := c.sortLookup(it, byPattern)
			if err != nil {
				return err
			}
			rows = append(rows, sortRow{val: it, str: w})
		}
	} else {
		for _, it := range items {
			rows = append(rows, sortRow{val: it, str: it})
		}
	}

	if !nosort {
		if !alpha {
			// Without ALPHA every weight must be a double, as in Redis. A
			// missing weight key yields an empty string, which counts as 0.
			for i := range rows {
				s := strings.TrimSpace(rows[i].str)
				if s == "" {
					rows[i].num = 0
					continue
				}
				v, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return &protoError{"ERR One or more scores can't be converted into double"}
				}
				rows[i].num = v
			}
		}
		sort.SliceStable(rows, func(i, j int) bool {
			var cmp int
			if alpha {
				cmp = strings.Compare(rows[i].str, rows[j].str)
			} else if rows[i].num < rows[j].num {
				cmp = -1
			} else if rows[i].num > rows[j].num {
				cmp = 1
			}
			// Equal weights fall back to the element itself, so ties are
			// deterministic (and lexicographic, as Redis does).
			if cmp == 0 {
				cmp = strings.Compare(rows[i].val, rows[j].val)
			}
			if desc {
				return cmp > 0
			}
			return cmp < 0
		})
	}

	// "BY nosort" keeps the source order, but DESC still reverses it.
	if nosort && desc {
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
	}

	// Apply LIMIT to the ordered result. COUNT may be huge (clients pass
	// i64::MAX for "the rest"), so compute against the remaining length
	// instead of the raw sum, which would overflow.
	if limitOff < 0 || limitOff > len(rows) {
		limitOff = len(rows)
	}
	end := len(rows)
	if limitCount >= 0 && limitCount < len(rows)-limitOff {
		end = limitOff + limitCount
	}
	rows = rows[limitOff:end]
	result := make([]string, 0, len(rows))
	for _, r := range rows {
		result = append(result, r.val)
	}

	if storeDst != "" {
		if len(result) == 0 {
			// Storing nothing removes the destination; the removal is still a
			// modification for WATCH purposes.
			if _, err := c.Store.deleteKey(c.DB, storeDst); err != nil {
				return err
			}
			c.writeInt(0)
			return nil
		}
		if _, err := c.Store.deleteKey(c.DB, storeDst); err != nil {
			return err
		}
		if _, err := c.Store.listPush(c.DB, storeDst, result, false, false); err != nil {
			return err
		}
		c.writeInt(int64(len(result)))
		return nil
	}

	// With GET the reply is one value per (element, pattern) pair, in pattern
	// order; a missing key replies nil.
	if len(getPatterns) > 0 {
		c.w.WriteArray(len(result) * len(getPatterns))
		for _, v := range result {
			for _, p := range getPatterns {
				s, found, err := c.sortLookupValue(v, p)
				if err != nil {
					return err
				}
				if !found {
					c.writeNull()
					continue
				}
				c.w.WriteBulkString(s)
			}
		}
		return nil
	}

	c.w.WriteArray(len(result))
	for _, s := range result {
		c.w.WriteBulkString(s)
	}
	return nil
}

// cmdSortRO is SORT_RO: the read-only SORT variant, which rejects STORE.
func cmdSortRO(c *Ctx, args [][]byte) error {
	for _, a := range args {
		if strings.EqualFold(string(a), "STORE") {
			return &protoError{"ERR SORT_RO is read-only and does not support the STORE option"}
		}
	}
	return cmdSort(c, args)
}

// sortLookup resolves a BY pattern for elem, returning "" when the weight key
// is missing (Redis treats a missing weight as 0).
func (c *Ctx) sortLookup(elem, pattern string) (string, error) {
	s, _, err := c.sortLookupValue(elem, pattern)
	return s, err
}

// sortLookupValue resolves a BY/GET pattern for elem: the pattern's "*" is
// replaced by the element, "key->field" reads a hash field, and "#" is the
// element itself. found reports whether the value exists.
func (c *Ctx) sortLookupValue(elem, pattern string) (string, bool, error) {
	if pattern == "#" {
		return elem, true, nil
	}
	key, field := pattern, ""
	fromHash := false
	// "->" only means a hash field when a field name follows it; a pattern
	// ending in "->" is a plain key that happens to contain the arrow.
	if i := strings.Index(pattern, "->"); i >= 0 && i+2 < len(pattern) {
		key, field, fromHash = pattern[:i], pattern[i+2:], true
	}
	key = strings.Replace(key, "*", elem, 1)

	if fromHash {
		a, err := c.Store.loadAgg(c.DB, key, config.TypeHash)
		if err != nil || !a.exists {
			// A missing (or wrong-typed) weight key is simply absent.
			return "", false, nil
		}
		v, ok := a.hash[field]
		if !ok {
			return "", false, nil
		}
		return string(v), true, nil
	}
	v, ok, err := c.Store.getString(c.DB, key)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	return string(v), true, nil
}

func cmdDel(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	var n int64
	for _, a := range args {
		key := string(a)
		if _, _, _, ok, err := c.Store.getTyped(c.DB, key); err != nil {
			return err
		} else if !ok {
			continue
		}
		if _, err := c.Store.deleteKey(c.DB, key); err != nil {
			return err
		}
		n++
	}
	c.writeInt(n)
	return nil
}

// cmdUnlink is the non-blocking flavour of DEL. There is no separate free path
// here - the value lives in Pebble - so it behaves identically.
func cmdUnlink(c *Ctx, args [][]byte) error { return cmdDel(c, args) }

func cmdExists(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = string(a)
	}
	n, err := c.Store.exists(c.DB, keys)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

func cmdType(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	typ, ok, err := c.Store.typeOf(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok {
		// Streams are stored outside the dict; check Pebble directly.
		if sok, serr := c.Store.streamExists(c.DB, string(args[0])); serr != nil {
			return serr
		} else if sok {
			c.w.WriteString("stream")
			return nil
		}
		c.w.WriteString("none")
		return nil
	}
	c.w.WriteString(config.TypeName(typ))
	return nil
}

func cmdTTL(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	ms := c.Store.ttlOf(c.DB, string(args[0]))
	switch ms {
	case -1, -2:
		c.writeInt(ms)
	default:
		// Redis rounds TTL up: a key with 1ms left reports 1 second, not 0.
		c.writeInt((ms + 999) / 1000)
	}
	return nil
}

func cmdPTTL(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	c.writeInt(c.Store.ttlOf(c.DB, string(args[0])))
	return nil
}

// expireAtCmd implements EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT.
func expireAtCmd(c *Ctx, key string, at time.Time, opt ExpireOption) error {
	n, err := c.Store.setExpiry(c.DB, key, at, opt)
	if err != nil {
		return err
	}
	c.writeInt(n)
	return nil
}

// expireAbsTime computes the absolute expiry time for a relative TTL in the
// given unit (mult=1000 for seconds, mult=1 for milliseconds), mirroring
// Redis' overflow checks: both the unit-scaled value and its sum with "now"
// must fit in a signed 64-bit millisecond timestamp, otherwise the TTL is out
// of range (e.g. EXPIRE key 9223370399119966).
func expireAbsTime(now time.Time, value, mult int64) (time.Time, error) {
	ms := value * mult
	if mult != 0 && value != 0 && ms/mult != value {
		return time.Time{}, ErrInvalidExpire
	}
	nowMs := now.UnixMilli()
	abs := nowMs + ms
	if (ms > 0 && abs < nowMs) || (ms < 0 && abs > nowMs) {
		return time.Time{}, ErrInvalidExpire
	}
	return time.UnixMilli(abs), nil
}

func cmdExpire(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	secs, err := toInt64(args[1])
	if err != nil {
		return err
	}
	at, err := expireAbsTime(c.Store.clock.Now(), secs, 1000)
	if err != nil {
		return err
	}
	opt, err := parseExpireOption(args, 2)
	if err != nil {
		return err
	}
	return expireAtCmd(c, string(args[0]), at, opt)
}

func cmdPExpire(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	ms, err := toInt64(args[1])
	if err != nil {
		return err
	}
	at, err := expireAbsTime(c.Store.clock.Now(), ms, 1)
	if err != nil {
		return err
	}
	opt, err := parseExpireOption(args, 2)
	if err != nil {
		return err
	}
	return expireAtCmd(c, string(args[0]), at, opt)
}

func cmdExpireAt(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	ts, err := toInt64(args[1])
	if err != nil {
		return err
	}
	opt, err := parseExpireOption(args, 2)
	if err != nil {
		return err
	}
	return expireAtCmd(c, string(args[0]), time.Unix(ts, 0), opt)
}

func cmdPExpireAt(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -2); err != nil {
		return err
	}
	ts, err := toInt64(args[1])
	if err != nil {
		return err
	}
	opt, err := parseExpireOption(args, 2)
	if err != nil {
		return err
	}
	return expireAtCmd(c, string(args[0]), time.UnixMilli(ts), opt)
}

func cmdPersist(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	ok, err := c.Store.persist(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	c.writeInt(btoi(ok))
	return nil
}

func cmdKeys(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	pattern := string(args[0])
	var match func(string) bool
	if pattern == "*" {
		match = func(string) bool { return true }
	} else {
		g, err := glob.Compile(pattern)
		if err != nil {
			return ErrSyntax
		}
		match = g.Match
	}

	// KEYS is O(N) by definition and must not be used in production; scanning
	// Pebble rather than materialising the key list keeps memory flat.
	var out [][]byte
	now := c.Store.clock.NowMilli()
	err := c.Store.eng.ScanKeys(storage.DataPrefix(c.DB), func(k []byte) error {
		key := storage.KeyFromDataKey(k)
		if key == "" {
			return nil
		}
		if !c.Store.keyAlive(c.DB, key, now) {
			return nil
		}
		if match(key) {
			out = append(out, []byte(key))
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.w.WriteArray(len(out))
	for _, k := range out {
		c.w.WriteBulk(k)
	}
	return nil
}

func cmdDBSize(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	// Streams live only in Pebble (no dict entry), so they are counted
	// separately and de-duplicated against the dict.
	n := int64(c.Store.dict.Len(c.DB))
	inDict := func(k string) bool {
		_, ok := c.Store.dict.Lookup(c.DB, k)
		return ok
	}
	prefix := streamEntryPrefix(c.DB, "")
	var last string
	_ = c.Store.eng.ScanKeys(prefix[:3], func(k []byte) error {
		// Entry keys are [seg][db][name]0x00[id]; the name ends at 0x00.
		name := k[3:]
		if i := bytes.IndexByte(name, 0x00); i >= 0 {
			name = name[:i]
		}
		if len(name) == 0 || string(name) == last {
			return nil
		}
		last = string(name)
		if !inDict(last) {
			n++
		}
		return nil
	})
	c.writeInt(n)
	return nil
}

func cmdFlushDB(c *Ctx, args [][]byte) error {
	if len(args) > 1 {
		return ErrSyntax
	}
	if len(args) == 1 && !strings.EqualFold(string(args[0]), "async") &&
		!strings.EqualFold(string(args[0]), "sync") {
		return ErrSyntax
	}
	if err := c.Store.flushDB(c.DB); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

func cmdFlushAll(c *Ctx, args [][]byte) error {
	if len(args) > 1 {
		return ErrSyntax
	}
	if len(args) == 1 && !strings.EqualFold(string(args[0]), "async") &&
		!strings.EqualFold(string(args[0]), "sync") {
		return ErrSyntax
	}
	for db := range c.Store.DBCount() {
		if err := c.Store.flushDB(uint16(db)); err != nil {
			return err
		}
	}
	c.writeOK()
	return nil
}

func cmdRename(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	if err := c.Store.rename(c.DB, string(args[0]), string(args[1]), false); err != nil {
		return err
	}
	c.writeOK()
	return nil
}

func cmdRenameNX(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	// RENAMENX reports 0 rather than failing when the target exists.
	if _, _, _, exists, err := c.Store.getTyped(c.DB, string(args[1])); err != nil {
		return err
	} else if exists {
		c.writeInt(0)
		return nil
	}
	if err := c.Store.rename(c.DB, string(args[0]), string(args[1]), true); err != nil {
		return err
	}
	c.writeInt(1)
	return nil
}

// scanCursorTTL is how long a SCAN cursor stays valid. A full iteration takes a
// fraction of this, but an abandoned iteration must not leak the cursor.
const scanCursorTTL = 5 * time.Minute

// maxScanCursors caps the table. It is generous: one entry per in-flight scan,
// and an entry is a handful of bytes.
const maxScanCursors = 4096

type scanCursorEntry struct {
	key    []byte
	db     uint16
	expiry time.Time
}

// scanCursorTable maps an opaque numeric SCAN cursor back to the last key
// handed out.
//
// Redis clients parse the cursor as an unsigned integer, so the wire value
// cannot be the key itself. Keeping the mapping server-side keeps a resumed
// scan at O(1) seek cost; the alternative — a cursor that counts how many keys
// to skip — would make a full iteration quadratic over the keyspace.
//
// A cursor that is unknown or expired is not an error: the scan simply restarts
// from the beginning, which SCAN already permits (it may return keys twice).
type scanCursorTable struct {
	mu   sync.Mutex
	next uint64
	byID map[uint64]scanCursorEntry
}

func newScanCursorTable() *scanCursorTable {
	return &scanCursorTable{byID: make(map[uint64]scanCursorEntry)}
}

// alloc stores key (already a copy owned by the caller) and returns its cursor.
func (t *scanCursorTable) alloc(key []byte, db uint16) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	if len(t.byID) >= maxScanCursors {
		// Drop expired entries first, then the oldest ids (ids increase
		// monotonically, so a small id means an old cursor).
		for id, e := range t.byID {
			if now.After(e.expiry) || id < t.next-maxScanCursors/2 {
				delete(t.byID, id)
			}
		}
	}

	t.next++
	if t.next == 0 {
		t.next = 1 // 0 is reserved for "start over" / "done"
	}
	id := t.next
	t.byID[id] = scanCursorEntry{key: key, db: db, expiry: now.Add(scanCursorTTL)}
	return id
}

// lookup returns the resume key for id, or nil when the cursor is unknown,
// expired or belongs to another database.
func (t *scanCursorTable) lookup(id uint64, db uint16) []byte {
	t.mu.Lock()
	defer t.mu.Unlock()

	e, ok := t.byID[id]
	if !ok || e.db != db || time.Now().After(e.expiry) {
		return nil
	}
	return e.key
}

// cmdScan implements SCAN. It returns up to COUNT keys per call and resumes
// from a cursor that encodes the last key returned.
//
// Redis' SCAN cursor is a position in the key space, not a skip count. We use
// the same model: each call seeks to the first key strictly greater than the
// last one it handed back, so iteration always advances toward the
// lexicographic end. A SCAN under concurrent writes still terminates — every
// key present for the whole scan is returned, and the cursor reaches 0 once the
// tail is exhausted. A "0" cursor (the only value a client may invent) means
// start from the beginning.
//
// The wire format is a plain decimal number: Redis hands the cursor back as a
// bulk string, but every client library (go-redis, redis-cli, jedis, …) parses
// it as an unsigned 64-bit integer, so a base64 or otherwise opaque cursor
// breaks every client. The numeric cursor is an id into a server-side table
// that remembers the last key, which keeps a resumed scan at O(1) seek cost —
// a skip-count cursor would make a full iteration quadratic over the keyspace.
func cmdScan(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	// The cursor is mandatory; everything after it comes in option/value pairs,
	// so the count is always odd.
	if (len(args)-1)%2 != 0 {
		return ErrSyntax
	}
	var startKey []byte
	if string(args[0]) != "0" {
		id, perr := strconv.ParseUint(string(args[0]), 10, 64)
		if perr != nil {
			return &protoError{"ERR invalid cursor"}
		}
		// A cursor we produced resolves back to the previous call's last key.
		// One we did not produce (garbage, or an entry since expired) restarts
		// from the beginning, which is the tolerant behaviour Redis shows.
		startKey = c.Store.scanCursors.lookup(id, c.DB)
	}
	var matchFn func(string) bool
	typeFilter := ""
	count := 10
	for i := 1; i < len(args); i += 2 {
		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			g, cerr := glob.Compile(string(args[i+1]))
			if cerr != nil {
				return ErrSyntax
			}
			matchFn = g.Match
		case "COUNT":
			cnt, perr := strconv.Atoi(string(args[i+1]))
			if perr != nil {
				return ErrNotInteger
			}
			if cnt <= 0 {
				return ErrSyntax
			}
			count = cnt
		case "TYPE":
			// The type must be given exactly (no pattern matching); an
			// unknown type simply filters everything out.
			typeFilter = strings.ToLower(string(args[i+1]))
		default:
			return ErrSyntax
		}
	}
	if matchFn == nil {
		matchFn = func(string) bool { return true }
	}

	batch := make([]string, 0, count)
	now := c.Store.clock.NowMilli()
	var lastKey []byte // last data key examined, used to build the next cursor
	stopped := false
	err := c.Store.eng.ScanKeysFrom(storage.DataPrefix(c.DB), startKey, func(k []byte) error {
		if len(batch) >= count {
			return storage.ErrStop
		}
		key := storage.KeyFromDataKey(k)
		if key == "" {
			return nil
		}
		lastKey = append([]byte(nil), k...)
		if !c.Store.keyAlive(c.DB, key, now) {
			return nil
		}
		if typeFilter != "" {
			typ, exists, terr := c.Store.typeOf(c.DB, key)
			if terr != nil {
				return terr
			}
			if !exists || config.TypeName(typ) != typeFilter {
				return nil
			}
		}
		if matchFn(key) {
			batch = append(batch, key)
		}
		return nil
	})
	if err != nil {
		if storage.IsStop(err) {
			stopped = true
		} else {
			return err
		}
	}

	// cursor 0 iff the iteration ran off the end; otherwise hand out an id for
	// the last key so the next call resumes just past it.
	nextCursor := "0"
	if stopped && len(lastKey) > 0 {
		nextCursor = strconv.FormatUint(c.Store.scanCursors.alloc(lastKey, c.DB), 10)
	}

	c.w.WriteArray(2)
	c.w.WriteBulkString(nextCursor)
	c.w.WriteArray(len(batch))
	for _, k := range batch {
		c.w.WriteBulkString(k)
	}
	return nil
}

// scanPageReply writes a [cursor, elements] SCAN-style reply from a fully
// materialised, MATCH-filtered flat array. cursor is the array-element offset to
// resume from (a decimal string, exactly like SCAN); count is the maximum number
// of array elements to return this call. An unknown or out-of-range cursor
// restarts from the beginning, which matches SCAN's tolerant semantics and lets
// every client (go-redis, redis-cli, jedis) resume correctly.
func scanPageReply(w Writer, flat []string, cursor uint64, count int) {
	if cursor >= uint64(len(flat)) {
		w.WriteArray(2)
		w.WriteBulkString("0")
		w.WriteArray(0)
		return
	}
	end := cursor + uint64(count)
	// end<cursor means the addition wrapped past uint64; clamp instead.
	if end > uint64(len(flat)) || end < cursor {
		end = uint64(len(flat))
	}
	page := flat[cursor:end]
	next := end
	if next >= uint64(len(flat)) {
		next = 0
	}
	w.WriteArray(2)
	w.WriteBulkString(strconv.FormatUint(next, 10))
	w.WriteArray(len(page))
	for _, s := range page {
		w.WriteBulkString(s)
	}
}

func cmdTouch(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), -1); err != nil {
		return err
	}
	var n int64
	for _, a := range args {
		if _, _, _, ok, err := c.Store.getTyped(c.DB, string(a)); err != nil {
			return err
		} else if ok {
			n++
		}
	}
	c.writeInt(n)
	return nil
}
