package compat

import "testing"

// Ported from SugarDB internal/modules/generic/commands_test.go
// (Test_HandleDEL). The DEL group is small - two cases - but its second
// assertion style (re-GET every key after the delete instead of trusting the
// reply) is worth keeping verbatim.

func TestPorted_DEL(t *testing.T) {
	c := setup(t)

	// Case 1: delete multiple keys, four of five exist; then verify each key's
	// existence independently instead of trusting the DEL reply alone.
	for k, v := range map[string]string{
		"DelKey1": "value1",
		"DelKey2": "value2",
		"DelKey3": "value3",
		"DelKey4": "value4",
	} {
		preset(t, c, k, v)
	}
	assertInt(t, do(t, c, "DEL", "DelKey1", "DelKey2", "DelKey3", "DelKey4", "DelKey5"), 4)
	for _, k := range []string{"DelKey1", "DelKey2", "DelKey3", "DelKey4", "DelKey5"} {
		if v := do(t, c, "GET", k); !v.IsNull() {
			t.Errorf("key %s survived DEL: %q", k, v.String())
		}
	}

	// Case 2: DEL with no keys is an arity error.
	assertErr(t, do(t, c, "DEL"), errWrongArgs)
}
