package redistore

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/redistore/redistore/config"
	"github.com/redistore/redistore/memory"
)

// M1 completion for list, generic and string commands.

// ---------- list: LMOVE / BLMOVE / LPOS / LMPOP / BLMPOP ----------

// listPopSide pops one element from the given side and returns it.
func (s *Store) listPopSide(db uint16, key string, left bool) (string, bool, error) {
	out, err := s.listPop(db, key, 1, left)
	if err != nil {
		return "", false, err
	}
	if len(out) == 0 {
		return "", false, nil
	}
	return out[0], true, nil
}

func cmdLMove(c *Ctx, args [][]byte) error  { return lmoveCmd(c, args, false) }
func cmdBLMove(c *Ctx, args [][]byte) error { return lmoveCmd(c, args, true) }

func lmoveCmd(c *Ctx, args [][]byte, blocking bool) error {
	want := 4
	if blocking {
		want = 5
	}
	if err := c.checkArgLen(len(args), want); err != nil {
		return err
	}
	var secs int64
	if blocking {
		var err error
		if secs, err = toInt64(args[3]); err != nil {
			return err
		}
		if secs < 0 {
			return &protoError{"ERR timeout is negative"}
		}
	}
	src, dst := string(args[0]), string(args[1])
	srcLeft, err := parseSide(args[2])
	if err != nil {
		return err
	}
	dstLeft, err := parseSide(args[3])
	if err != nil {
		return err
	}

	timeout := time.Duration(secs) * time.Second
	deadline := time.Time{}
	if blocking && secs > 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		bvsrcVersion := c.Store.blockVersion()
		v, ok, err := c.Store.listPopSide(c.DB, src, srcLeft)
		if err != nil {
			return err
		}
		if ok {
			if _, err := c.Store.listPush(c.DB, dst, []string{v}, dstLeft, false); err != nil {
				return err
			}
			c.w.WriteBulkString(v)
			return nil
		}
		if !blocking {
			c.writeNull()
			return nil
		}
		if secs > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				c.writeNull()
				return nil
			}
			timeout = remaining
		}
		if !c.Store.blockSleepSince(bvsrcVersion, timeout) {
			c.writeNull()
			return nil
		}
		bvsrcVersion = c.Store.blockVersion()
	}
}

// boolToInt exists so the arity check above can index args[3] for BLMOVE and
// args[2]... no: LMOVE takes (src, dst, from, to), BLMOVE adds timeout first.
func parseSide(b []byte) (bool, error) {
	switch strings.ToUpper(string(b)) {
	case "LEFT":
		return true, nil
	case "RIGHT":
		return false, nil
	}
	return false, ErrSyntax
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func cmdLPos(c *Ctx, args [][]byte) error {
	if len(args) < 2 {
		return WrongArgs("lpos")
	}
	key := string(args[0])
	elem := string(args[1])
	rank, count, maxLen := 1, 1, 0
	hasCount := false
	for i := 2; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return ErrSyntax
		}
		switch strings.ToUpper(string(args[i])) {
		case "RANK":
			v, err := toInt64(args[i+1])
			if err != nil || v == 0 {
				return &protoError{"ERR RANK can't be zero. Use 1 to start searching from the first, matching element in the head, or -1 to start searching from the tail."}
			}
			rank = int(v)
		case "COUNT":
			v, err := toInt64(args[i+1])
			if err != nil {
				return err
			}
			count, hasCount = int(v), true
			if count < 0 {
				return &protoError{"ERR COUNT can't be negative"}
			}
		case "MAXLEN":
			v, err := toInt64(args[i+1])
			if err != nil || v < 0 {
				return &protoError{"ERR MAXLEN can't be negative"}
			}
			maxLen = int(v)
		default:
			return ErrSyntax
		}
	}

	a, err := c.Store.loadAgg(c.DB, key, config.TypeList)
	if err != nil {
		return err
	}
	n := len(a.list)
	var matches []int
	if rank > 0 {
		skip := rank - 1
		for i := 0; i < n && (maxLen == 0 || i < maxLen); i++ {
			if a.list[i] != elem {
				continue
			}
			if skip > 0 {
				skip--
				continue
			}
			matches = append(matches, i)
			// COUNT 0 means "all matches", so only a positive count stops early.
			if hasCount && count > 0 && len(matches) >= count {
				break
			}
		}
	} else {
		skip := -rank - 1
		for i := n - 1; i >= 0 && (maxLen == 0 || n-1-i < maxLen); i-- {
			if a.list[i] != elem {
				continue
			}
			if skip > 0 {
				skip--
				continue
			}
			matches = append(matches, i)
			if hasCount && count > 0 && len(matches) >= count {
				break
			}
		}
	}

	if !hasCount {
		if len(matches) == 0 {
			c.writeNull()
			return nil
		}
		c.writeInt(int64(matches[0]))
		return nil
	}
	c.w.WriteArray(len(matches))
	for _, i := range matches {
		c.writeInt(int64(i))
	}
	return nil
}

// cmdLMPop pops from the first non-empty list among keys.
func cmdLMPop(c *Ctx, args [][]byte) error  { return lmpopCmd(c, args, false) }
func cmdBLMPop(c *Ctx, args [][]byte) error { return lmpopCmd(c, args, true) }

func lmpopCmd(c *Ctx, args [][]byte, blocking bool) error {
	minArgs := 2
	if blocking {
		minArgs = 3
	}
	if len(args) < minArgs {
		return WrongArgs("lmpop")
	}
	nkPos := 0
	var secs int64
	if blocking {
		var err error
		if secs, err = toInt64(args[0]); err != nil {
			return err
		}
		if secs < 0 {
			return &protoError{"ERR timeout is negative"}
		}
		nkPos = 1
	}
	nk, err := toInt64(args[nkPos])
	if err != nil || nk <= 0 || int(nk) > len(args)-nkPos-1 {
		return &protoError{"ERR numkeys should be greater than 0 and no larger than the number of keys"}
	}
	keys := byteSliceToStrings(args[nkPos+1 : nkPos+1+int(nk)])
	rest := args[nkPos+1+int(nk):]
	left := true
	count := 1
	for i := 0; i < len(rest); i++ {
		switch strings.ToUpper(string(rest[i])) {
		case "LEFT":
			left = true
		case "RIGHT":
			left = false
		case "COUNT":
			if i+1 >= len(rest) {
				return ErrSyntax
			}
			v, perr := toInt64(rest[i+1])
			if perr != nil {
				return perr
			}
			count = int(v)
			i++
		}
	}

	timeout := time.Duration(secs) * time.Second
	deadline := time.Time{}
	if blocking && secs > 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		bvkeysVersion := c.Store.blockVersion()
		for _, k := range keys {
			a, err := c.Store.loadAgg(c.DB, k, config.TypeList)
			if err != nil {
				return err
			}
			if len(a.list) == 0 {
				continue
			}
			out, err := c.Store.listPop(c.DB, k, count, left)
			if err != nil {
				return err
			}
			c.w.WriteArray(2)
			c.w.WriteBulkString(k)
			c.w.WriteArray(len(out))
			for _, e := range out {
				c.w.WriteBulkString(e)
			}
			return nil
		}
		if !blocking {
			c.writeNull()
			return nil
		}
		if secs > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				c.writeNull()
				return nil
			}
			timeout = remaining
		}
		if !c.Store.blockSleepSince(bvkeysVersion, timeout) {
			c.writeNull()
			return nil
		}
		bvkeysVersion = c.Store.blockVersion()
	}
}

// ---------- generic: COPY / MOVE / RANDOMKEY / EXPIRETIME ----------

// copyKey duplicates a key, including every element of an aggregate.
func (s *Store) copyKey(srcDB uint16, src string, dstDB uint16, dst string, replace bool) (bool, error) {
	if srcDB == dstDB && src == dst && !replace {
		return false, nil
	}
	if !replace {
		if _, _, _, ok, err := s.getTyped(dstDB, dst); err != nil {
			return false, err
		} else if ok {
			return false, nil
		}
	}
	payload, typ, expireAt, ok, err := s.getTyped(srcDB, src)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if typ != config.TypeString {
		a, err := s.loadAgg(srcDB, src, typ)
		if err != nil {
			return false, err
		}
		if _, err := s.deleteKey(dstDB, dst); err != nil {
			return false, err
		}
		na := &agg{typ: typ, exists: true, sparse: a.sparse, expireAt: expireAt,
			hash: a.hash, set: a.set, zset: a.zset, list: a.list, seqs: a.seqs,
			head: a.head, tail: a.tail}
		if err := s.saveAgg(dstDB, dst, na); err != nil {
			return false, err
		}
		return true, nil
	}
	prevExpire := s.oldExpiry(dstDB, dst)
	if err := s.putValue(dstDB, dst, typ, payload, expireAt, prevExpire); err != nil {
		return false, err
	}
	return true, nil
}

func cmdCopy(c *Ctx, args [][]byte) error {
	if len(args) < 2 {
		return WrongArgs("copy")
	}
	dstDB := c.DB
	replace := false
	dst := string(args[1])
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "DB":
			if i+1 >= len(args) {
				return ErrSyntax
			}
			v, err := toInt64(args[i+1])
			if err != nil || !c.Store.dict.Valid(uint16(v)) {
				return ErrDBIndex
			}
			dstDB = uint16(v)
			i++ // the database id was consumed
		case "REPLACE":
			replace = true
		default:
			return ErrSyntax
		}
	}
	ok, err := c.Store.copyKey(c.DB, string(args[0]), dstDB, dst, replace)
	if err != nil {
		return err
	}
	c.writeInt(btoi(ok))
	return nil
}

func cmdMove(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	db, err := toInt64(args[1])
	if err != nil || !c.Store.dict.Valid(uint16(db)) {
		return ErrDBIndex
	}
	dstDB := uint16(db)
	key := string(args[0])
	ok, err := c.Store.copyKey(c.DB, key, dstDB, key, true)
	if err != nil || !ok {
		if err == nil {
			c.writeInt(0)
		}
		return err
	}
	// COPY into the same name but a different database; now drop the source.
	if c.DB != dstDB {
		if _, err := c.Store.deleteKey(c.DB, key); err != nil {
			return err
		}
	}
	c.writeInt(1)
	return nil
}

func cmdRandomKey(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 0); err != nil {
		return err
	}
	// Scan databases round-robin style starting at a random one so no single
	// database monopolises the result.
	start := c.Store.randIntn(c.Store.DBCount())
	for i := range c.Store.DBCount() {
		db := (start + i) % c.Store.DBCount()
		if k, ok := c.Store.dict.RandomKey(uint16(db)); ok {
			c.w.WriteBulkString(k)
			return nil
		}
	}
	c.writeNull()
	return nil
}

func cmdExpireTime(c *Ctx, args [][]byte) error  { return expireTimeCmd(c, args, false) }
func cmdPExpireTime(c *Ctx, args [][]byte) error { return expireTimeCmd(c, args, true) }

func expireTimeCmd(c *Ctx, args [][]byte, ms bool) error {
	if err := c.checkArgLen(len(args), 1); err != nil {
		return err
	}
	_, _, expireAt, ok, err := c.Store.getTyped(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if !ok {
		c.writeInt(-2)
		return nil
	}
	if expireAt == 0 {
		c.writeInt(-1)
		return nil
	}
	if ms {
		c.writeInt(expireAt)
	} else {
		c.writeInt(expireAt / 1000)
	}
	return nil
}

// ---------- string: INCRBYFLOAT / LCS ----------

func cmdIncrByFloat(c *Ctx, args [][]byte) error {
	if err := c.checkArgLen(len(args), 2); err != nil {
		return err
	}
	cur, typ, expireAt, ok, err := c.Store.getTyped(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	if ok && typ != config.TypeString {
		return ErrWrongType
	}
	inc, err := toFloat(args[1])
	if err != nil {
		return err
	}
	var curF float64
	if ok {
		if curF, err = toFloat(cur); err != nil {
			return err
		}
	}
	next := curF + inc
	if next >= 1e308 || next <= -1e308 {
		return &protoError{"ERR increment would produce NaN or Infinity"}
	}
	if err := c.Store.setStringSimple(c.DB, string(args[0]), []byte(formatFloatTrim(next)), expireAt); err != nil {
		return err
	}
	c.w.WriteBulkString(formatFloatTrim(next))
	return nil
}

// formatFloatTrim renders a float the way INCRBYFLOAT does: fixed notation,
// trailing zeros trimmed.
func formatFloatTrim(f float64) string {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if s == "" || s == "-" {
		return "0"
	}
	return s
}

// cmdLCS implements the longest common subsequence of two strings.
func cmdLCS(c *Ctx, args [][]byte) error {
	if len(args) < 2 {
		return WrongArgs("lcs")
	}
	withIdx, withLen := false, false
	minMatch := 1
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "IDX":
			withIdx = true
		case "LEN":
			withLen = true
		case "MINMATCHLEN":
			if i+1 >= len(args) {
				return ErrSyntax
			}
			v, err := toInt64(args[i+1])
			if err != nil || v < 0 {
				return ErrNotInteger
			}
			minMatch = int(v)
			i++
		case "WITHMATCHLEN":
			withLen = true
		default:
			return ErrSyntax
		}
	}
	a, ok1, err := c.Store.getString(c.DB, string(args[0]))
	if err != nil {
		return err
	}
	b, ok2, err := c.Store.getString(c.DB, string(args[1]))
	if err != nil {
		return err
	}
	if !ok1 || !ok2 {
		c.writeNull()
		return nil
	}

	// Guard: LCS is O(len1*len2) memory in IDX mode; cap the work.
	if int64(len(a))*int64(len(b)) > 64*1024*1024 {
		return &protoError{"ERR string too long for LCS"}
	}
	matches, lcsLen := lcsCompute(string(a), string(b), minMatch)

	if withLen && !withIdx {
		c.writeInt(int64(lcsLen))
		return nil
	}
	if !withIdx {
		c.w.WriteBulkString(lcsString(string(a), string(b), matches))
		return nil
	}
	// IDX mode: {"matches": [[a_start, a_end, b_start, b_end [, len]]...], "len": n}
	c.w.WriteArray(2)
	c.w.WriteBulkString("matches")
	c.w.WriteArray(len(matches))
	for _, m := range matches {
		n := 4
		if withLen {
			n = 5
		}
		c.w.WriteArray(n)
		c.writeInt(int64(m.aStart))
		c.writeInt(int64(m.aEnd))
		c.writeInt(int64(m.bStart))
		c.writeInt(int64(m.bEnd))
		if withLen {
			c.writeInt(int64(m.aEnd - m.aStart + 1))
		}
	}
	c.w.WriteBulkString("len")
	c.writeInt(int64(lcsLen))
	return nil
}

type lcsMatch struct{ aStart, aEnd, bStart, bEnd int }

// lcsCompute returns the common non-overlapping match ranges and total length.
func lcsCompute(a, b string, minMatch int) ([]lcsMatch, int) {
	na, nb := len(a), len(b)
	// dp[i][j] = LCS length of a[i:], b[j:]. Two rolling rows are enough for
	// lengths; reconstructing ranges needs the full walk, done greedily here.
	type cell struct{ l, next int }
	dp := make([][]cell, na+1)
	for i := range dp {
		dp[i] = make([]cell, nb+1)
	}
	for i := na - 1; i >= 0; i-- {
		for j := nb - 1; j >= 0; j-- {
			if a[i] == b[j] {
				dp[i][j].l = dp[i+1][j+1].l + 1
				dp[i][j].next = 0 // diagonal
			} else if dp[i+1][j].l >= dp[i][j+1].l {
				dp[i][j].l = dp[i+1][j].l
				dp[i][j].next = 1 // down
			} else {
				dp[i][j].l = dp[i][j+1].l
				dp[i][j].next = 2 // right
			}
		}
	}
	var matches []lcsMatch
	total := 0
	i, j := 0, 0
	for i < na && j < nb {
		if a[i] == b[j] {
			ai, bj := i, j
			for i < na && j < nb && a[i] == b[j] {
				i++
				j++
			}
			if i-ai >= minMatch {
				matches = append(matches, lcsMatch{ai, i - 1, bj, j - 1})
				total += i - ai
			}
			continue
		}
		switch dp[i][j].next {
		case 1:
			i++
		default:
			j++
		}
	}
	return matches, total
}

func lcsString(a, b string, matches []lcsMatch) string {
	out := make([]byte, 0)
	for _, m := range matches {
		out = append(out, a[m.aStart:m.aEnd+1]...)
	}
	return string(out)
}

// randIntn exposes the RNG for RANDOMKEY's database pick.
func (s *Store) randIntn(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.IntN(n)
}

var _ = memory.Idle
