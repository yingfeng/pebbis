package compat

import (
	"testing"

	"github.com/tidwall/resp"
)

// Ported from SugarDB internal/modules/list/commands_test.go
// (Test_HandleLLEN, Test_HandleLINDEX).
//
// One deviation is baked into how the fixtures are built: SugarDB presets lists
// with `LPUSH key v1 v2 v3 v4` and then expects LINDEX 3 == "value4", i.e. its
// LPUSH keeps the argument order. Redis applies each element in turn, so the
// list ends up reversed and LINDEX 3 would be "value1". These fixtures use
// RPUSH instead, which produces the same intended list while matching Redis.

// presetList seeds a list via RPUSH and asserts the length.
func presetList(t *testing.T, c *resp.Conn, key string, elems ...string) {
	t.Helper()
	args := append([]string{"RPUSH", key}, elems...)
	assertInt(t, do(t, c, args...), len(elems))
}

func TestPorted_LLEN(t *testing.T) {
	c := setup(t)

	tests := []struct {
		name      string
		key       string
		preset    []string
		presetStr string
		command   []string
		want      int
		wantErr   string
	}{
		{
			name:    "1. If key exists and is a list, return the lists length",
			key:     "LlenKey1",
			preset:  []string{"value1", "value2", "value3", "value4"},
			command: []string{"LLEN", "LlenKey1"},
			want:    4,
		},
		{
			name:    "2. If key does not exist, return 0",
			key:     "LlenKey2",
			command: []string{"LLEN", "LlenKey2"},
			want:    0,
		},
		{
			name:    "3. Command too short",
			key:     "LlenKey3",
			command: []string{"LLEN"},
			wantErr: errWrongArgs,
		},
		{
			name:    "4. Command too long",
			key:     "LlenKey4",
			command: []string{"LLEN", "LlenKey4", "LlenKey4"},
			wantErr: errWrongArgs,
		},
		{
			name:      "5. Getting the length of a non-list is a type error",
			key:       "LlenKey5",
			presetStr: "Default value",
			command:   []string{"LLEN", "LlenKey5"},
			wantErr:   errWrongType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset != nil {
				presetList(t, c, tt.key, tt.preset...)
			}
			if tt.presetStr != "" {
				preset(t, c, tt.key, tt.presetStr)
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

func TestPorted_LINDEX(t *testing.T) {
	c := setup(t)

	tests := []struct {
		name      string
		key       string
		preset    []string
		presetStr string
		command   []string
		want      string
		wantNil   bool
		wantErr   string
	}{
		{
			name:    "1. Return last element within range",
			key:     "LindexKey1",
			preset:  []string{"value1", "value2", "value3", "value4"},
			command: []string{"LINDEX", "LindexKey1", "3"},
			want:    "value4",
		},
		{
			name:   "2. Return first element within range",
			key:    "LindexKey2",
			preset: []string{"value1", "value2", "value3", "value4"},
			// NOTE: SugarDB indexes key "LindexKey1" here (a fixture typo);
			// the intent is clearly the first element of this key.
			command: []string{"LINDEX", "LindexKey2", "0"},
			want:    "value1",
		},
		{
			name:    "3. Return middle element within range",
			key:     "LindexKey3",
			preset:  []string{"value1", "value2", "value3", "value4"},
			command: []string{"LINDEX", "LindexKey3", "1"},
			want:    "value2",
		},
		{
			name:    "4. If key does not exist, return nil",
			key:     "LindexKey4",
			command: []string{"LINDEX", "LindexKey4", "0"},
			wantNil: true,
		},
		{
			name:    "5. If the index is -1, return the element from the end of the list",
			key:     "LindexKey5",
			preset:  []string{"value1", "value2", "value3", "value4", "value5"},
			command: []string{"LINDEX", "LindexKey5", "-1"},
			want:    "value5",
		},
		{
			name:    "6. If index is -3, return the 3rd element from the end of the list",
			key:     "LindexKey6",
			preset:  []string{"value1", "value2", "value3", "value4", "value5"},
			command: []string{"LINDEX", "LindexKey6", "-3"},
			want:    "value3",
		},
		{
			name:    "7. When the negative index exceeds the list length, return nil",
			key:     "LindexKey7",
			preset:  []string{"value1", "value2", "value3", "value4", "value5"},
			command: []string{"LINDEX", "LindexKey7", "-10"},
			wantNil: true,
		},
		{
			name:      "8. Indexing a non-list is a type error",
			key:       "LindexKey8",
			presetStr: "Default value",
			command:   []string{"LINDEX", "LindexKey8", "0"},
			wantErr:   errWrongType,
		},
		{
			name:    "9. Index beyond the last element returns nil",
			key:     "LindexKey9",
			preset:  []string{"value1", "value2", "value3"},
			command: []string{"LINDEX", "LindexKey9", "3"},
			wantNil: true,
		},
		{
			name:    "10. Return error when index is not an integer",
			key:     "LindexKey10",
			preset:  []string{"value1", "value2", "value3"},
			command: []string{"LINDEX", "LindexKey10", "index"},
			wantErr: errNotInt,
		},
		{
			name:    "11. Command too short",
			key:     "LindexKey11",
			command: []string{"LINDEX", "LindexKey11"},
			wantErr: errWrongArgs,
		},
		{
			name:    "12. Command too long",
			key:     "LindexKey12",
			command: []string{"LINDEX", "LindexKey12", "0", "20"},
			wantErr: errWrongArgs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset != nil {
				presetList(t, c, tt.key, tt.preset...)
			}
			if tt.presetStr != "" {
				preset(t, c, tt.key, tt.presetStr)
			}
			res := do(t, c, tt.command...)
			if tt.wantErr != "" {
				assertErr(t, res, tt.wantErr)
				return
			}
			if tt.wantNil {
				if !res.IsNull() {
					t.Errorf("expected nil, got %q", res.String())
				}
				return
			}
			assertStr(t, res, tt.want)
		})
	}
}
