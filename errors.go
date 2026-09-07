package redistore

import (
	"errors"
	"fmt"
	"strings"
)

// Protocol-level errors. The wording matches Redis so that existing clients and
// their error handlers behave identically.
var (
	// ErrWrongType is returned when a command meets a key of another type.
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	// ErrSyntax is returned for malformed commands.
	ErrSyntax = errors.New("ERR syntax error")
	// ErrNotInteger is returned when a numeric argument does not parse.
	ErrNotInteger = errors.New("ERR value is not an integer or out of range")
	// ErrNotFloat is the float counterpart of ErrNotInteger.
	ErrNotFloat = errors.New("ERR value is not a valid float")
	// ErrOverflow is returned when INCR/DECR would leave the 64-bit range.
	ErrOverflow = errors.New("ERR increment or decrement would overflow")
	// ErrNoSuchKey is returned by commands that require an existing key.
	ErrNoSuchKey = errors.New("ERR no such key")
	// ErrDBIndex is returned when SELECT targets a database out of range.
	ErrDBIndex = errors.New("ERR DB index is out of range")
	// ErrInvalidExpire is returned for out-of-range TTLs.
	ErrInvalidExpire = errors.New("ERR invalid expire time in 'set' command")
	// ErrUnknownCmd is returned for unrecognised commands.
	ErrUnknownCmd = errors.New("ERR unknown command")
	// ErrQuit is a sentinel that ends the connection.
	ErrQuit = errors.New("quit")
	// ErrBusyKey is returned when a write is rejected by NX/XX semantics.
	ErrBusyKey = errors.New("BUSYKEY Target key name already exists")
)

// WrongArgs builds the standard arity error for a command.
func WrongArgs(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd))
}

// UnknownCommand builds the error for an unrecognised command name.
func UnknownCommand(name string) error {
	return fmt.Errorf("ERR unknown command '%s', with args beginning with: ", name)
}

// OOM is returned when no eviction can free memory.
func OOM(cmd string) error {
	return fmt.Errorf("OOM command not allowed when used memory > 'maxmemory'. (command: %s)", cmd)
}

// IsQuit reports whether err asks for the connection to be closed.
func IsQuit(err error) bool {
	return errors.Is(err, ErrQuit)
}
