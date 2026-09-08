package pebbis

import (
	"strconv"
	"strings"
)

// commandKeys returns the key arguments a command touches, in the order they
// appear in args (which already excludes the command name). It mirrors kvrocks'
// Command::ForEachKeyRange: dispatch locks every key the command may read or
// write, so the command's whole read-modify-write runs under a consistent set of
// per-key locks (see keyLockTable.lockAll, which orders them to avoid deadlock).
//
// Commands whose keys follow no simple pattern are handled by name; everything
// else defaults to a single key (the first argument), which is correct for the
// vast majority of single-key commands (GET, SET, HSET, SADD, ZADD, ...).
func commandKeys(name string, args [][]byte) []string {
	switch name {
	// The whole argument set is keys.
	case "DEL", "UNLINK", "EXISTS", "TOUCH", "MGET",
		"SUNION", "SINTER", "SDIFF", "SINTERCARD":
		return stringsOf(args)

	// MSET / MSETNX: key, value, key, value, ...
	case "MSET", "MSETNX":
		out := make([]string, 0, len(args)/2+1)
		for i := 0; i+1 < len(args); i += 2 {
			out = append(out, string(args[i]))
		}
		return out

	// Two explicit keys.
	case "RENAME", "RENAMENX", "COPY", "SMOVE", "RPOPLPUSH", "LMOVE", "BLMOVE",
		"ZRANGESTORE":
		if len(args) < 2 {
			return nil
		}
		return []string{string(args[0]), string(args[1])}

	// Set-algebra stores: dest is args[0], then numkeys sources.
	case "SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE":
		return keysWithNumkeys(args, 1)

	// Sorted-set algebra stores: dest args[0], numkeys args[1].
	case "ZINTERSTORE", "ZUNIONSTORE", "ZDIFFSTORE":
		return keysWithNumkeys(args, 1)

	// ZMPOP / BZMPOP and ZUNION/ZINTER/ZDIFF: numkeys args[0], then keys.
	case "ZMPOP", "BZMPOP", "ZUNION", "ZINTER", "ZDIFF":
		return keysWithNumkeys(args, 0)

	// BITOP: op args[0], dest args[1], sources args[2:].
	case "BITOP":
		if len(args) < 2 {
			return nil
		}
		return stringsOf(args[1:])

	// SORT / SORT_RO: key args[0], optional "STORE dest".
	case "SORT", "SORT_RO":
		return sortKeys(args)

	// No key-level locking: whole-DB operations, connection/server commands,
	// pub/sub, or commands whose "arguments" are not keys (SWAPDB swaps DBs,
	// MOVE's second arg is a DB number handled per-DB).
	case "FLUSHDB", "FLUSHALL", "DBSIZE", "KEYS", "RANDOMKEY", "SCAN",
		"SWAPDB", "SELECT", "PING", "ECHO", "QUIT", "COMMAND",
		"HELLO", "AUTH", "CONFIG", "DEBUG", "SAVE", "BGSAVE", "LASTSAVE",
		"TIME", "MEMORY", "SLOWLOG", "CLIENT", "OBJECT", "SHUTDOWN",
		"SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE",
		"PUBLISH", "PUBSUB", "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH":
		return nil

	// Default: a single key (the first argument).
	default:
		if len(args) == 0 {
			return nil
		}
		return []string{string(args[0])}
	}
}

// stringsOf converts a [][]byte of arguments to []string.
func stringsOf(args [][]byte) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		out = append(out, string(a))
	}
	return out
}

// keysWithNumkeys returns the keys following a "numkeys" count at args[numIdx].
// Used by *STORE commands and the *MPOP family. A malformed count yields nil so
// the caller simply takes no lock (the command will fail its own arity check).
func keysWithNumkeys(args [][]byte, numIdx int) []string {
	if numIdx >= len(args) {
		return nil
	}
	n, err := strconv.Atoi(string(args[numIdx]))
	if err != nil || n < 0 {
		return nil
	}
	start := numIdx + 1
	end := start + n
	if end > len(args) {
		end = len(args)
	}
	return stringsOf(args[start:end])
}

// sortKeys returns the SORT key plus an optional STORE destination.
func sortKeys(args [][]byte) []string {
	if len(args) == 0 {
		return nil
	}
	out := []string{string(args[0])}
	for i := 1; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "STORE") && i+1 < len(args) {
			out = append(out, string(args[i+1]))
			break
		}
	}
	return out
}
