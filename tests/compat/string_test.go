package compat

import (
	"strconv"
	"testing"

	"github.com/tidwall/resp"
)

// Ported from SugarDB internal/modules/string/commands_test.go.
// Expectations were re-derived against real Redis where SugarDB diverges; see
// the NOTE comments.

func TestSetRange(t *testing.T) {
	c := setup(t)

	tests := []struct {
		name        string
		key         string
		presetValue string
		command     []string
		wantValue   string
		wantLen     int
		wantErr     string
	}{
		{
			name:    "SETRANGE on non-existent key pads with zero bytes",
			key:     "SetRangeKey1",
			command: []string{"SETRANGE", "SetRangeKey1", "10", "New String Value"},
			// NOTE: SugarDB expected len(value)=16. Redis pads the gap, so the
			// result is 10 zero bytes followed by the value: 26 bytes.
			wantLen: 10 + len("New String Value"),
		},
		{
			name:        "SETRANGE with an offset that leads to a longer resulting string",
			key:         "SetRangeKey2",
			presetValue: "Original String Value",
			command:     []string{"SETRANGE", "SetRangeKey2", "16", "Portion Replaced With This New String"},
			wantValue:   "Original String Portion Replaced With This New String",
			wantLen:     len("Original String Portion Replaced With This New String"),
		},
		{
			name:        "SETRANGE with negative offset is rejected",
			key:         "SetRangeKey3",
			presetValue: "This is a preset value",
			command:     []string{"SETRANGE", "SetRangeKey3", "-10", "Prepended "},
			// NOTE: SugarDB prepends on a negative offset. Redis rejects it.
			wantErr: errOutOfRange,
		},
		{
			name:        "SETRANGE with offset that embeds new string inside the old string",
			key:         "SetRangeKey4",
			presetValue: "This is a preset value",
			command:     []string{"SETRANGE", "SetRangeKey4", "0", "That"},
			wantValue:   "That is a preset value",
			wantLen:     len("That is a preset value"),
		},
		{
			name:        "SETRANGE with offset past the end pads with zero bytes",
			key:         "SetRangeKey5",
			presetValue: "This is a preset value",
			command:     []string{"SETRANGE", "SetRangeKey5", "100", " Appended"},
			// NOTE: SugarDB expected a contiguous string of length 31. Redis
			// pads with zero bytes out to the offset, giving 109 bytes.
			wantLen: 100 + len(" Appended"),
		},
		{
			name:        "SETRANGE with offset on the last character replaces the tail",
			key:         "SetRangeKey6",
			presetValue: "This is a preset value",
			command:     []string{"SETRANGE", "SetRangeKey6", strconv.Itoa(len("This is a preset value") - 1), " replaced"},
			wantValue:   "This is a preset valu replaced",
			wantLen:     len("This is a preset valu replaced"),
		},
		{
			name:    "Offset not an integer",
			command: []string{"SETRANGE", "key", "offset", "value"},
			wantErr: errNotInt,
		},
		{
			name:    "Command too short",
			command: []string{"SETRANGE", "key"},
			wantErr: errWrongArgs,
		},
		{
			name:    "Command too long",
			command: []string{"SETRANGE", "key", "offset", "value", "value1"},
			wantErr: errWrongArgs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presetValue != "" {
				preset(t, c, tt.key, tt.presetValue)
			}
			res := do(t, c, tt.command...)
			if tt.wantErr != "" {
				assertErr(t, res, tt.wantErr)
				return
			}
			assertInt(t, res, tt.wantLen)

			if tt.wantValue != "" {
				got := do(t, c, "GET", tt.key)
				assertStr(t, got, tt.wantValue)
			}
		})
	}
}

func TestStrLen(t *testing.T) {
	c := setup(t)

	tests := []struct {
		name        string
		key         string
		presetValue string
		command     []string
		want        int
		wantErr     string
	}{
		{
			name:        "Return the correct string length for an existing string",
			key:         "StrLenKey1",
			presetValue: "Test String",
			command:     []string{"STRLEN", "StrLenKey1"},
			want:        len("Test String"),
		},
		{
			name:    "If the string does not exist, return 0",
			key:     "StrLenKey2",
			command: []string{"STRLEN", "StrLenKey2"},
			want:    0,
		},
		{
			name:    "Too few args",
			command: []string{"STRLEN"},
			wantErr: errWrongArgs,
		},
		{
			name:    "Too many args",
			command: []string{"STRLEN", "StrLenKey4", "StrLenKey5"},
			wantErr: errWrongArgs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presetValue != "" {
				preset(t, c, tt.key, tt.presetValue)
			}
			res := do(t, c, tt.command...)
			if tt.wantErr != "" {
				assertErr(t, res, tt.wantErr)
				return
			}
			assertInt(t, res, tt.want)
		})
	}
}

func TestSubStr(t *testing.T) {
	c := setup(t)

	tests := []struct {
		name        string
		key         string
		presetValue string
		command     []string
		want        string
		wantErr     string
	}{
		{
			name:        "Return substring within the range of the string",
			key:         "SubStrKey1",
			presetValue: "Test String One",
			command:     []string{"SUBSTR", "SubStrKey1", "5", "10"},
			want:        "String",
		},
		{
			name:        "Return substring at the end of the string with exact end index",
			key:         "SubStrKey2",
			presetValue: "Test String Two",
			command:     []string{"SUBSTR", "SubStrKey2", "12", "14"},
			want:        "Two",
		},
		{
			name:        "Return substring at the end of the string with end index greater than length",
			key:         "SubStrKey3",
			presetValue: "Test String Three",
			command:     []string{"SUBSTR", "SubStrKey3", "12", "75"},
			want:        "Three",
		},
		{
			name:        "Return the substring at the start of the string with 0 start index",
			key:         "SubStrKey4",
			presetValue: "Test String Four",
			command:     []string{"SUBSTR", "SubStrKey4", "0", "3"},
			want:        "Test",
		},
		{
			name:        "Return the substring with negative start index",
			key:         "SubStrKey5",
			presetValue: "Test String Five",
			command:     []string{"SUBSTR", "SubStrKey5", "-11", "10"},
			want:        "String",
		},
		{
			name:        "End index smaller than start index yields an empty string",
			key:         "SubStrKey6",
			presetValue: "Test String Six",
			command:     []string{"SUBSTR", "SubStrKey6", "4", "0"},
			// NOTE: SugarDB reverses the indices and returns "tseT". Redis
			// returns an empty bulk string when start > end.
			want: "",
		},
		{
			name:    "Command too short",
			command: []string{"SUBSTR", "key", "10"},
			wantErr: errWrongArgs,
		},
		{
			name:    "Command too long",
			command: []string{"SUBSTR", "key", "10", "15", "20"},
			wantErr: errWrongArgs,
		},
		{
			name:    "Start index is not an integer",
			command: []string{"SUBSTR", "key", "start", "10"},
			wantErr: errNotInt,
		},
		{
			name:    "End index is not an integer",
			command: []string{"SUBSTR", "key", "0", "end"},
			wantErr: errNotInt,
		},
		{
			name:    "Non-existent key returns an empty string",
			command: []string{"SUBSTR", "non-existent-key", "0", "10"},
			// NOTE: SugarDB errors here. Redis returns an empty string.
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presetValue != "" {
				preset(t, c, tt.key, tt.presetValue)
			}
			res := do(t, c, tt.command...)
			if tt.wantErr != "" {
				assertErr(t, res, tt.wantErr)
				return
			}
			assertStr(t, res, tt.want)
		})
	}
}

func TestAppend(t *testing.T) {
	c := setup(t)

	tests := []struct {
		name        string
		key         string
		presetValue string
		command     []string
		want        int
		wantErr     string
	}{
		{
			name:    "APPEND with no preset value",
			key:     "AppendKey1",
			command: []string{"APPEND", "AppendKey1", "Hello"},
			want:    5,
		},
		{
			name:        "APPEND with preset value",
			key:         "AppendKey2",
			presetValue: "Hello ",
			command:     []string{"APPEND", "AppendKey2", "World"},
			want:        11,
		},
		{
			name:        "APPEND to a value holding digits appends to the digits",
			key:         "AppendKey4",
			presetValue: "10",
			command:     []string{"APPEND", "AppendKey4", "World"},
			// NOTE: SugarDB stores integers as a distinct type and errors.
			// Redis keeps everything as a string, so this yields "10World".
			want: len("10World"),
		},
		{
			name:    "Command too short",
			command: []string{"APPEND", "AppendKey5"},
			wantErr: errWrongArgs,
		},
		{
			name:    "Command too long",
			command: []string{"APPEND", "AppendKey5", "new value", "extra value"},
			wantErr: errWrongArgs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.presetValue != "" {
				preset(t, c, tt.key, tt.presetValue)
			}
			res := do(t, c, tt.command...)
			if tt.wantErr != "" {
				assertErr(t, res, tt.wantErr)
				return
			}
			assertInt(t, res, tt.want)
		})
	}
}

// TestSetGetRangeRoundTrip covers GETRANGE, the canonical spelling of SUBSTR.
func TestSetGetRangeRoundTrip(t *testing.T) {
	c := setup(t)
	preset(t, c, "g", "Hello World")

	assertStr(t, do(t, c, "GETRANGE", "g", "0", "4"), "Hello")
	assertStr(t, do(t, c, "GETRANGE", "g", "-5", "-1"), "World")
	assertStr(t, do(t, c, "GETRANGE", "g", "0", "-7"), "Hello")
}

func TestPingEcho(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "PING"), "PONG")
	assertStr(t, do(t, c, "ECHO", "hello"), "hello")
	assertStr(t, do(t, c, "PING", "hi"), "hi")
	assertErr(t, do(t, c, "ECHO"), errWrongArgs)
	assertErr(t, do(t, c, "ECHO", "a", "b"), errWrongArgs)
	assertErr(t, do(t, c, "NOSUCHCOMMAND"), errUnknownCmd)
}

func TestSelectDBIndex(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "SELECT", "0"), "OK")
	assertErr(t, do(t, c, "SELECT", "99"), errDBIndex)
	assertErr(t, do(t, c, "SELECT"), errWrongArgs)
}

func TestTypeCommand(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "TYPE", "missing"), "none")
	preset(t, c, "s", "v")
	assertStr(t, do(t, c, "TYPE", "s"), "string")
	assertErr(t, do(t, c, "TYPE"), errWrongArgs)
}

func TestKeyCounting(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "EXISTS", "a", "b"), 0)
	preset(t, c, "a", "1")
	assertInt(t, do(t, c, "EXISTS", "a", "b"), 1)
	preset(t, c, "b", "2")
	assertInt(t, do(t, c, "EXISTS", "a", "b"), 2)
	assertInt(t, do(t, c, "DBSIZE"), 2)
	assertInt(t, do(t, c, "DEL", "a", "b", "zz"), 2)
	assertInt(t, do(t, c, "DBSIZE"), 0)
}

func TestUnknownAndArity(t *testing.T) {
	c := setup(t)
	// Unknown commands must not desynchronise the protocol: the connection
	// stays usable afterwards.
	assertErr(t, do(t, c, "BOGUS", "x"), errUnknownCmd)
	assertStr(t, do(t, c, "PING"), "PONG")
}

func TestSetNXAndXX(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "SETNX", "k", "v1"), 1)
	assertInt(t, do(t, c, "SETNX", "k", "v2"), 0)
	assertStr(t, do(t, c, "GET", "k"), "v1")
	assertInt(t, do(t, c, "SETXX", "k", "v3"), 1)
	assertStr(t, do(t, c, "GET", "k"), "v3")
	assertInt(t, do(t, c, "SETXX", "absent", "v"), 0)
}

func TestMSetMGetProtocol(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "MSET", "a", "1", "b", "2"), "OK")
	assertErr(t, do(t, c, "MSET", "a", "1", "b"), errWrongArgs)

	got := do(t, c, "MGET", "a", "b", "missing")
	if got.Type() != resp.Array {
		t.Fatalf("MGET should return an array, got %s", got.Type())
	}
	items := got.Array()
	if len(items) != 3 {
		t.Fatalf("MGET returned %d items, want 3", len(items))
	}
	assertStr(t, items[0], "1")
	assertStr(t, items[1], "2")
	if !items[2].IsNull() {
		t.Errorf("missing key should be nil, got %s %q", items[2].Type(), items[2].String())
	}
}

func TestGetDelAndGetSet(t *testing.T) {
	c := setup(t)
	preset(t, c, "k", "old")

	assertStr(t, do(t, c, "GETSET", "k", "new"), "old")
	assertStr(t, do(t, c, "GET", "k"), "new")
	assertStr(t, do(t, c, "GETDEL", "k"), "new")
	if v := do(t, c, "GET", "k"); !v.IsNull() {
		t.Errorf("GETDEL should remove the key, got %q", v.String())
	}
}
