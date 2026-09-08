// Package glob implements Redis-compatible key-pattern matching for KEYS,
// SCAN MATCH, and pub/sub channel patterns.
//
// Unlike github.com/gobwas/glob, the `{` and `}` characters are treated as
// literal (Redis never does brace alternation in KEYS patterns); only `*`, `?`,
// `[...]` (with `^`/`!` negation and `a-z` ranges) and `\` escaping are
// special.
//
// The matcher is built around a segment scan: the pattern is split on `*`
// into literal runs that must appear in order. A leading/trailing literal run
// is anchored to the string start/end, the middle runs are matched greedily at
// their earliest occurrence. This is O(pattern_length + string_length) for the
// nested-loop patterns Redis uses as regression tests (e.g. "a*a*...*b" and
// "*?*?...*?") instead of the exponential backtracking a naive recursive glob
// would suffer.
package glob

// Glob is a compiled pattern that can test strings for a match.
type Glob interface {
	Match(s string) bool
}

type compiled struct {
	pattern string
}

func (c compiled) Match(s string) bool {
	return match(c.pattern, s)
}

// Compile compiles a Redis-style glob pattern. It never fails; a malformed
// pattern simply matches nothing meaningful, as in Redis.
func Compile(pattern string) (Glob, error) {
	return compiled{pattern: pattern}, nil
}

// maxStarCount mirrors Redis's GLOB_MATCH_MAX_RECURSION (100), which bounds
// the number of '*' groups stringmatchlen may descend into. Beyond this the
// matcher gives up and reports no-match, turning the catastrophic-backtracking
// nested-loop patterns (e.g. "*?" repeated 50000 times) into a fast, defined
// "no match" instead of a CPU hang.
const maxStarCount = 100

// match reports whether str matches pattern under Redis glob semantics.
func match(pattern, str string) bool {
	if pattern == "" {
		return str == ""
	}
	if countStars(pattern) > maxStarCount {
		return false
	}
	if !containsStar(pattern) {
		// No wildcards: the pattern must match the whole string.
		ok, consumed := matchAt(pattern, str, 0)
		return ok && consumed == len(str)
	}

	// Split into literal runs separated by '*'. A run may contain '?', '[...]'
	// and escaped literals but never '*' (those were the separators).
	segs := splitPattern(pattern)
	startAnchored := pattern[0] != '*'
	endAnchored := pattern[len(pattern)-1] != '*'

	pos := 0
	if startAnchored {
		// The first run must match at the very beginning.
		ok, consumed := matchAt(segs[0], str, 0)
		if !ok {
			return false
		}
		pos = consumed
		segs = segs[1:]
	}

	for idx, seg := range segs {
		if seg == "" {
			// Consecutive stars or a leading/trailing star: matches anything.
			continue
		}
		isLast := idx == len(segs)-1
		found := -1
		for i := pos; i <= len(str); i++ {
			ok, consumed := matchAt(seg, str, i)
			if !ok {
				continue
			}
			if isLast && endAnchored {
				// Anchor the final run to the string end.
				if i+consumed == len(str) {
					found = i
					break
				}
				continue
			}
			found = i
			pos = i + consumed
			break
		}
		if found < 0 {
			return false
		}
		if isLast && endAnchored {
			pos = len(str)
		}
	}
	return true
}

func containsStar(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '*' {
			return true
		}
	}
	return false
}

// countStars returns the number of '*' characters in s.
func countStars(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '*' {
			n++
		}
	}
	return n
}

// splitPattern splits pattern on '*' into literal runs.
func splitPattern(pattern string) []string {
	out := make([]string, 0, 1)
	start := 0
	for i := 0; i < len(pattern); i++ {
		if pattern[i] == '*' {
			out = append(out, pattern[start:i])
			start = i + 1
		}
	}
	out = append(out, pattern[start:])
	return out
}

// matchAt reports whether seg matches str[i:] with '?', '[...]' and '\'
// support. It returns whether the run matched and how many string characters
// it consumed. The caller guarantees i is in bounds.
func matchAt(seg, str string, i int) (bool, int) {
	si := i // string cursor, advanced once per matched character
	for k := 0; k < len(seg); k++ {
		if si >= len(str) {
			return false, 0
		}
		pc := seg[k]
		sc := str[si]
		switch pc {
		case '?':
			// Matches any single character.
			si++
		case '[':
			ok, nk := matchClass(seg, k, sc)
			if !ok {
				return false, 0
			}
			si++
			k = nk // nk points at ']'; the loop's k++ steps past it.
		case '\\':
			k++
			if k >= len(seg) || seg[k] != sc {
				return false, 0
			}
			si++
		default:
			if pc != sc {
				return false, 0
			}
			si++
		}
	}
	return true, si - i
}

// matchClass parses a "[...]" class starting at seg[k] ('[') against byte sc.
// It returns whether sc is in the class and the index of the closing ']' so the
// caller can advance past it.
func matchClass(seg string, k int, sc byte) (bool, int) {
	j := k + 1
	negate := false
	if j < len(seg) && (seg[j] == '^' || seg[j] == '!') {
		negate = true
		j++
	}
	matched := false
	for j < len(seg) && seg[j] != ']' {
		if seg[j] == '\\' && j+1 < len(seg) {
			j++
		}
		lo := seg[j]
		j++
		hi := lo
		if j < len(seg) && seg[j] == '-' && j+1 < len(seg) && seg[j+1] != ']' {
			j++
			hi = seg[j]
			j++
		}
		if lo <= sc && sc <= hi {
			matched = true
		}
	}
	// j is at ']' or at the end of seg if unterminated.
	return matched != negate, j
}
