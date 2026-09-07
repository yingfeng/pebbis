package redistore

import (
	"strconv"
	"strings"
	"time"
)

// SetOption carries the parsed flags of SET and friends.
//
// The shape mirrors cybergarage/go-redis's redis.SetOption, which got the
// Redis option grammar right; re-deriving it from the spec is a reliable way to
// end up subtly incompatible with real clients.
type SetOption struct {
	// EX / PX are relative durations.
	EX time.Duration
	PX time.Duration
	// EXAT / PXAT are absolute deadlines.
	EXAT time.Time
	PXAT time.Time
	// NX / XX are the conditional-write flags.
	NX bool
	XX bool
	// KEEPTTL preserves any existing TTL on overwrite.
	KEEPTTL bool
	// GET returns the previous value.
	GET bool
}

// ExpireAt resolves the effective expiry of the option set, or the zero time
// when no expiry was requested.
func (o SetOption) ExpireAt(now time.Time) time.Time {
	switch {
	case o.EX > 0:
		return now.Add(o.EX)
	case o.PX > 0:
		return now.Add(o.PX)
	case !o.EXAT.IsZero():
		return o.EXAT
	case !o.PXAT.IsZero():
		return o.PXAT
	}
	return time.Time{}
}

// ExpireOption carries the parsed flags of EXPIRE / PEXPIRE / EXPIREAT / ...
type ExpireOption struct {
	// Time is the absolute deadline.
	Time time.Time
	// NX only sets the TTL when the key has none; XX only when it has one.
	NX bool
	XX bool
	// GT only extends an existing TTL; LT only shortens it.
	GT bool
	LT bool
}

// parseSetOption parses the trailing options of SET starting at args[i].
func parseSetOption(args [][]byte, i int) (SetOption, error) {
	var opt SetOption
	for ; i < len(args); i++ {
		arg := args[i]
		switch {
		case foldEqual(arg, "EX"):
			v, err := nextInt(args, &i, "EX")
			if err != nil {
				return opt, err
			}
			if v <= 0 {
				return opt, ErrInvalidExpire
			}
			opt.EX = time.Duration(v) * time.Second
		case foldEqual(arg, "PX"):
			v, err := nextInt(args, &i, "PX")
			if err != nil {
				return opt, err
			}
			if v <= 0 {
				return opt, ErrInvalidExpire
			}
			opt.PX = time.Duration(v) * time.Millisecond
		case foldEqual(arg, "EXAT"):
			v, err := nextInt(args, &i, "EXAT")
			if err != nil {
				return opt, err
			}
			if v <= 0 {
				return opt, ErrInvalidExpire
			}
			opt.EXAT = time.Unix(v, 0)
		case foldEqual(arg, "PXAT"):
			v, err := nextInt(args, &i, "PXAT")
			if err != nil {
				return opt, err
			}
			if v <= 0 {
				return opt, ErrInvalidExpire
			}
			opt.PXAT = time.UnixMilli(v)
		case foldEqual(arg, "KEEPTTL"):
			opt.KEEPTTL = true
		case foldEqual(arg, "NX"):
			opt.NX = true
		case foldEqual(arg, "XX"):
			opt.XX = true
		case foldEqual(arg, "GET"):
			opt.GET = true
		default:
			return opt, ErrSyntax
		}
	}
	if opt.NX && opt.XX {
		return opt, ErrSyntax
	}
	if opt.KEEPTTL && (opt.EX > 0 || opt.PX > 0 || !opt.EXAT.IsZero() || !opt.PXAT.IsZero()) {
		return opt, ErrSyntax
	}
	return opt, nil
}

// parseExpireOption parses the trailing flags shared by the EXPIRE family.
func parseExpireOption(args [][]byte, i int) (ExpireOption, error) {
	var opt ExpireOption
	for ; i < len(args); i++ {
		arg := args[i]
		switch {
		case foldEqual(arg, "NX"):
			opt.NX = true
		case foldEqual(arg, "XX"):
			opt.XX = true
		case foldEqual(arg, "GT"):
			opt.GT = true
		case foldEqual(arg, "LT"):
			opt.LT = true
		default:
			return opt, ErrSyntax
		}
	}
	if opt.NX && opt.XX {
		return opt, ErrSyntax
	}
	if opt.GT && opt.LT {
		return opt, ErrSyntax
	}
	if opt.NX && (opt.GT || opt.LT) {
		return opt, ErrSyntax
	}
	return opt, nil
}

// nextInt advances i and returns the following argument as an integer.
func nextInt(args [][]byte, i *int, name string) (int64, error) {
	*i++
	if *i >= len(args) {
		return 0, ErrSyntax
	}
	v, err := strconv.ParseInt(string(args[*i]), 10, 64)
	if err != nil {
		return 0, ErrNotInteger
	}
	_ = name
	return v, nil
}

// foldEqual compares a command argument case-insensitively.
func foldEqual(b []byte, s string) bool {
	return strings.EqualFold(string(b), s)
}

// toInt64 parses a command argument as a 64-bit integer.
func toInt64(b []byte) (int64, error) {
	v, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, ErrNotInteger
	}
	return v, nil
}

// toFloat parses a command argument as a float64.
func toFloat(b []byte) (float64, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
	if err != nil {
		return 0, ErrNotFloat
	}
	return v, nil
}
