package compat

import (
	"strings"
	"testing"
)

// Server-level smoke checks: introspection commands and RESP3 negotiation.

func TestHelloFallsBackToRESP2(t *testing.T) {
	c := setup(t)

	// A client probing for RESP3 must get a rejection it can fall back from,
	// not a protocol error that desynchronises the connection.
	res := do(t, c, "HELLO", "3")
	if res.Type().String() != "Error" && !strings.Contains(res.String(), "NOPROTO") {
		t.Errorf("HELLO 3 should report NOPROTO, got %q", res.String())
	}

	// HELLO with no argument describes the server.
	res = do(t, c, "HELLO")
	fields := res.Array()
	if len(fields) != 4 {
		t.Fatalf("HELLO returned %d fields, want 4", len(fields))
	}
	if fields[3].Integer() != 2 {
		t.Errorf("HELLO should advertise proto 2, got %d", fields[3].Integer())
	}

	// The connection still works afterwards.
	assertStr(t, do(t, c, "PING"), "PONG")
}

func TestCommandCountAndList(t *testing.T) {
	c := setup(t)
	n := do(t, c, "COMMAND", "COUNT").Integer()
	if n <= 0 {
		t.Fatalf("COMMAND COUNT returned %d", n)
	}
	names := do(t, c, "COMMAND", "LIST").Array()
	if len(names) != n {
		t.Errorf("COMMAND LIST returned %d names, COUNT reported %d", len(names), n)
	}
}

func TestInfoSections(t *testing.T) {
	c := setup(t)
	preset(t, c, "k", "v")

	for _, section := range []string{"", "server", "memory", "persistence", "stats", "keyspace"} {
		args := []string{"INFO"}
		if section != "" {
			args = append(args, section)
		}
		info := do(t, c, args...).String()
		if info == "" {
			t.Fatalf("INFO %s returned nothing", section)
		}
		if !strings.Contains(info, "#") {
			t.Errorf("INFO %s missing section header: %q", section, info)
		}
	}

	// The keyspace section should report the key we wrote.
	info := do(t, c, "INFO", "keyspace").String()
	if !strings.Contains(info, "db0") {
		t.Errorf("INFO keyspace should mention db0, got %q", info)
	}
}

func TestInfoReportsStorageEngine(t *testing.T) {
	c := setup(t)
	info := do(t, c, "INFO", "persistence").String()
	if !strings.Contains(info, "pebble") {
		t.Errorf("INFO persistence should name the storage engine, got %q", info)
	}
}

func TestTimeAndSave(t *testing.T) {
	c := setup(t)

	ts := do(t, c, "TIME").Array()
	if len(ts) != 2 {
		t.Fatalf("TIME returned %d elements, want 2", len(ts))
	}
	if ts[0].Integer() <= 0 {
		t.Errorf("TIME seconds = %d", ts[0].Integer())
	}
	assertStr(t, do(t, c, "SAVE"), "OK")
	if do(t, c, "LASTSAVE").Integer() <= 0 {
		t.Error("LASTSAVE should be a positive timestamp after SAVE")
	}
}

func TestConfigGet(t *testing.T) {
	c := setup(t)
	res := do(t, c, "CONFIG", "GET", "maxmemory-policy").Array()
	if len(res) != 2 {
		t.Fatalf("CONFIG GET returned %d elements, want 2", len(res))
	}
	if res[0].String() != "maxmemory-policy" {
		t.Errorf("CONFIG GET returned %q", res[0].String())
	}
	res = do(t, c, "CONFIG", "GET", "databases").Array()
	if res[1].Integer() != 16 {
		t.Errorf("databases = %d, want 16", res[1].Integer())
	}
}

func TestMemoryUsage(t *testing.T) {
	c := setup(t)
	preset(t, c, "k", "v")
	if n := do(t, c, "MEMORY", "USAGE", "k").Integer(); n <= 0 {
		t.Errorf("MEMORY USAGE = %d, want > 0", n)
	}
	if v := do(t, c, "MEMORY", "USAGE", "absent"); !v.IsNull() {
		t.Errorf("MEMORY USAGE on a missing key should be nil, got %q", v.String())
	}
}
