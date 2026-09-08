package tcl

// Hand-ported tests from frogdb crates/redis-regression/tests/set_tcl.rs
// (Redis 8.6.0 unit/type/set.tcl). Previously skip-only stubs emitted by the
// mechanical converter; written by hand against the real Rust source.

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func Test_TCL_tcl_sadd_scard_sismember_smismember_smembers_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myset")
	cDo(t, client, "SADD", "myset", "foo")
	assertIntegerEq(t, cDo(t, client, "SADD", "myset", "bar"), 1)
	assertIntegerEq(t, cDo(t, client, "SADD", "myset", "bar"), 0)
	assertIntegerEq(t, cDo(t, client, "SCARD", "myset"), 2)
	assertIntegerEq(t, cDo(t, client, "SISMEMBER", "myset", "foo"), 1)
	assertIntegerEq(t, cDo(t, client, "SISMEMBER", "myset", "bar"), 1)
	assertIntegerEq(t, cDo(t, client, "SISMEMBER", "myset", "bla"), 0)

	items := unwrapArray(t, cDo(t, client, "SMISMEMBER", "myset", "foo"))
	assertIntegerEq(t, items[0], 1)

	items = unwrapArray(t, cDo(t, client, "SMISMEMBER", "myset", "foo", "bar"))
	assertIntegerEq(t, items[0], 1)
	assertIntegerEq(t, items[1], 1)

	items = unwrapArray(t, cDo(t, client, "SMISMEMBER", "myset", "foo", "bla"))
	assertIntegerEq(t, items[0], 1)
	assertIntegerEq(t, items[1], 0)

	members := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "myset"))
	sort.Strings(members)
	assertStringsEq(t, members, "bar", "foo")
}

func Test_TCL_tcl_sadd_scard_sismember_intset(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myset")
	cDo(t, client, "SADD", "myset", "17")
	assertIntegerEq(t, cDo(t, client, "SADD", "myset", "16"), 1)
	assertIntegerEq(t, cDo(t, client, "SADD", "myset", "16"), 0)
	assertIntegerEq(t, cDo(t, client, "SCARD", "myset"), 2)
	assertIntegerEq(t, cDo(t, client, "SISMEMBER", "myset", "16"), 1)
	assertIntegerEq(t, cDo(t, client, "SISMEMBER", "myset", "17"), 1)
	assertIntegerEq(t, cDo(t, client, "SISMEMBER", "myset", "18"), 0)

	members := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "myset"))
	sort.Strings(members)
	assertStringsEq(t, members, "16", "17")
}

func Test_TCL_tcl_smismember_requires_one_or_more_members(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	assertErrorPrefix(t, cDo(t, client, "SMISMEMBER", "myset"), "ERR")
}

func Test_TCL_tcl_smismember_smembers_scard_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	items := unwrapArray(t, cDo(t, client, "SMISMEMBER", "myset1", "foo"))
	assertIntegerEq(t, items[0], 0)

	items = unwrapArray(t, cDo(t, client, "SMISMEMBER", "myset1", "foo", "bar"))
	assertIntegerEq(t, items[0], 0)
	assertIntegerEq(t, items[1], 0)

	items = unwrapArray(t, cDo(t, client, "SMEMBERS", "myset1"))
	if len(items) != 0 {
		t.Fatalf("expected empty members, got %v", items)
	}

	assertIntegerEq(t, cDo(t, client, "SCARD", "myset1"), 0)
}

func Test_TCL_tcl_variadic_sadd(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myset")
	assertIntegerEq(t, cDo(t, client, "SADD", "myset", "a", "b", "c"), 3)
	assertIntegerEq(t, cDo(t, client, "SADD", "myset", "A", "a", "b", "c", "B"), 2)
	members := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "myset"))
	sort.Strings(members)
	assertStringsEq(t, members, "A", "B", "a", "b", "c")
}

func Test_TCL_tcl_srem_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myset")
	cDo(t, client, "SADD", "myset", "foo", "ciao")
	assertIntegerEq(t, cDo(t, client, "SREM", "myset", "qux"), 0)
	assertIntegerEq(t, cDo(t, client, "SREM", "myset", "ciao"), 1)
	members := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "myset"))
	sort.Strings(members)
	assertStringsEq(t, members, "foo")
}

func Test_TCL_tcl_srem_intset(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myset")
	cDo(t, client, "SADD", "myset", "3", "4", "5")
	assertIntegerEq(t, cDo(t, client, "SREM", "myset", "6"), 0)
	assertIntegerEq(t, cDo(t, client, "SREM", "myset", "4"), 1)
	members := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "myset"))
	sort.Strings(members)
	assertStringsEq(t, members, "3", "5")
}

func Test_TCL_tcl_srem_with_multiple_arguments(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myset")
	cDo(t, client, "SADD", "myset", "a", "b", "c", "d")
	assertIntegerEq(t, cDo(t, client, "SREM", "myset", "k", "k", "k"), 0)
	assertIntegerEq(t, cDo(t, client, "SREM", "myset", "b", "d", "x", "y"), 2)
	members := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "myset"))
	sort.Strings(members)
	assertStringsEq(t, members, "a", "c")
}

func Test_TCL_tcl_sinter_with_two_sets(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "set1{t}", "set2{t}")
	for i := 0; i < 200; i++ {
		cDo(t, client, "SADD", "set1{t}", strconv.Itoa(i))
		cDo(t, client, "SADD", "set2{t}", strconv.Itoa(i+195))
	}
	result := extractBulkStrings(t, cDo(t, client, "SINTER", "set1{t}", "set2{t}"))
	sort.Slice(result, func(i, j int) bool {
		a, _ := strconv.Atoi(result[i])
		b, _ := strconv.Atoi(result[j])
		return a < b
	})
	assertStringsEq(t, result, "195", "196", "197", "198", "199")
}

func Test_TCL_tcl_sintercard_with_two_sets(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "set1{t}", "set2{t}")
	for i := 0; i < 200; i++ {
		cDo(t, client, "SADD", "set1{t}", strconv.Itoa(i))
		cDo(t, client, "SADD", "set2{t}", strconv.Itoa(i+195))
	}
	assertIntegerEq(t, cDo(t, client, "SINTERCARD", "2", "set1{t}", "set2{t}"), 5)
	assertIntegerEq(t, cDo(t, client, "SINTERCARD", "2", "set1{t}", "set2{t}", "LIMIT", "3"), 3)
	assertIntegerEq(t, cDo(t, client, "SINTERCARD", "2", "set1{t}", "set2{t}", "LIMIT", "10"), 5)
}

func Test_TCL_tcl_sinterstore_with_two_sets(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "set1{t}", "set2{t}", "setres{t}")
	for i := 0; i < 200; i++ {
		cDo(t, client, "SADD", "set1{t}", strconv.Itoa(i))
		cDo(t, client, "SADD", "set2{t}", strconv.Itoa(i+195))
	}
	cDo(t, client, "SINTERSTORE", "setres{t}", "set1{t}", "set2{t}")
	result := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "setres{t}"))
	sort.Slice(result, func(i, j int) bool {
		a, _ := strconv.Atoi(result[i])
		b, _ := strconv.Atoi(result[j])
		return a < b
	})
	assertStringsEq(t, result, "195", "196", "197", "198", "199")
}

func Test_TCL_tcl_sdiff_with_two_sets(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "set1{t}", "set4{t}")
	for i := 0; i < 200; i++ {
		cDo(t, client, "SADD", "set1{t}", strconv.Itoa(i))
	}
	for i := 5; i < 200; i++ {
		cDo(t, client, "SADD", "set4{t}", strconv.Itoa(i))
	}
	result := extractBulkStrings(t, cDo(t, client, "SDIFF", "set1{t}", "set4{t}"))
	sort.Slice(result, func(i, j int) bool {
		a, _ := strconv.Atoi(result[i])
		b, _ := strconv.Atoi(result[j])
		return a < b
	})
	assertStringsEq(t, result, "0", "1", "2", "3", "4")
}

func Test_TCL_tcl_sdiff_should_handle_non_existing_key_as_empty(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "set1{t}", "set2{t}", "set3{t}")
	cDo(t, client, "SADD", "set1{t}", "a", "b", "c")
	cDo(t, client, "SADD", "set2{t}", "b", "c", "d")
	result := extractBulkStrings(t, cDo(t, client, "SDIFF", "set1{t}", "set2{t}", "set3{t}"))
	sort.Strings(result)
	assertStringsEq(t, result, "a")
}

func Test_TCL_tcl_sunion_should_handle_non_existing_key_as_empty(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "set1{t}", "set2{t}", "set3{t}")
	cDo(t, client, "SADD", "set1{t}", "a", "b", "c")
	cDo(t, client, "SADD", "set2{t}", "b", "c", "d")
	result := extractBulkStrings(t, cDo(t, client, "SUNION", "set1{t}", "set2{t}", "set3{t}"))
	sort.Strings(result)
	assertStringsEq(t, result, "a", "b", "c", "d")
}

func Test_TCL_tcl_sinter_with_same_integer_elements_but_different_encoding(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "set1{t}", "set2{t}")
	cDo(t, client, "SADD", "set1{t}", "1", "2", "3")
	cDo(t, client, "SADD", "set2{t}", "1", "2", "3", "a")
	cDo(t, client, "SREM", "set2{t}", "a")
	result := extractBulkStrings(t, cDo(t, client, "SINTER", "set1{t}", "set2{t}"))
	sort.Strings(result)
	assertStringsEq(t, result, "1", "2", "3")
}

func Test_TCL_tcl_spop_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myset")
	cDo(t, client, "SADD", "myset", "a", "b", "c")
	popped := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		popped = append(popped, unwrapBulk(t, cDo(t, client, "SPOP", "myset")))
	}
	sort.Strings(popped)
	assertStringsEq(t, popped, "a", "b", "c")
	assertIntegerEq(t, cDo(t, client, "SCARD", "myset"), 0)
}

func Test_TCL_tcl_spop_with_count(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myset")
	for c := 'a'; c <= 'z'; c++ {
		cDo(t, client, "SADD", "myset", string(c))
	}
	assertIntegerEq(t, cDo(t, client, "SCARD", "myset"), 26)

	items := unwrapArray(t, cDo(t, client, "SPOP", "myset", "11"))
	if len(items) != 11 {
		t.Fatalf("expected 11 popped, got %d", len(items))
	}
	items = unwrapArray(t, cDo(t, client, "SPOP", "myset", "9"))
	if len(items) != 9 {
		t.Fatalf("expected 9 popped, got %d", len(items))
	}
	items = unwrapArray(t, cDo(t, client, "SPOP", "myset", "0"))
	if len(items) != 0 {
		t.Fatalf("expected 0 popped, got %d", len(items))
	}
	assertIntegerEq(t, cDo(t, client, "SCARD", "myset"), 6)
}

func Test_TCL_tcl_smove_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myset1{t}", "myset2{t}", "myset3{t}")
	cDo(t, client, "SADD", "myset1{t}", "1", "a", "b")
	cDo(t, client, "SADD", "myset2{t}", "2", "3", "4")
	assertIntegerEq(t, cDo(t, client, "SMOVE", "myset1{t}", "myset2{t}", "a"), 1)

	s1 := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "myset1{t}"))
	sort.Strings(s1)
	assertStringsEq(t, s1, "1", "b")
	s2 := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "myset2{t}"))
	sort.Strings(s2)
	assertStringsEq(t, s2, "2", "3", "4", "a")
}

func Test_TCL_tcl_smove_to_non_existing_destination_set(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myset1{t}", "myset3{t}")
	cDo(t, client, "SADD", "myset1{t}", "1", "a", "b")
	assertIntegerEq(t, cDo(t, client, "SMOVE", "myset1{t}", "myset3{t}", "a"), 1)

	s1 := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "myset1{t}"))
	sort.Strings(s1)
	assertStringsEq(t, s1, "1", "b")
	s3 := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "myset3{t}"))
	assertStringsEq(t, s3, "a")
}

func Test_TCL_tcl_smove_wrong_dst_key_type(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "x{t}", "myset1{t}")
	cDo(t, client, "SET", "x{t}", "10")
	cDo(t, client, "SADD", "myset1{t}", "a")
	assertErrorPrefix(t, cDo(t, client, "SMOVE", "myset1{t}", "x{t}", "foo"), "WRONGTYPE")
}

func Test_TCL_tcl_smove_with_identical_source_and_destination(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "set{t}")
	cDo(t, client, "SADD", "set{t}", "a", "b", "c")
	cDo(t, client, "SMOVE", "set{t}", "set{t}", "b")
	members := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "set{t}"))
	sort.Strings(members)
	assertStringsEq(t, members, "a", "b", "c")
}

func Test_TCL_tcl_sdiffstore_should_handle_non_existing_key_as_empty(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "set1{t}", "set2{t}", "set3{t}", "setres{t}")

	// Both non-existing -> result is empty, dstkey deleted.
	cDo(t, client, "SET", "setres{t}", "xxx")
	assertIntegerEq(t, cDo(t, client, "SDIFFSTORE", "setres{t}", "foo111{t}", "bar222{t}"), 0)
	assertIntegerEq(t, cDo(t, client, "EXISTS", "setres{t}"), 0)

	// set1 has elements, set2 empty.
	cDo(t, client, "SADD", "set1{t}", "a", "b", "c")
	assertIntegerEq(t, cDo(t, client, "SDIFFSTORE", "set3{t}", "set1{t}", "set2{t}"), 3)
	members := extractBulkStrings(t, cDo(t, client, "SMEMBERS", "set3{t}"))
	sort.Strings(members)
	assertStringsEq(t, members, "a", "b", "c")
}

func Test_TCL_tcl_spop_propagate_as_del_or_unlink(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	// Single SPOP removing last element.
	cDo(t, client, "DEL", "myset")
	cDo(t, client, "SADD", "myset", "a")
	assertIntegerEq(t, cDo(t, client, "SCARD", "myset"), 1)
	assertBulkEq(t, cDo(t, client, "SPOP", "myset"), "a")
	assertIntegerEq(t, cDo(t, client, "EXISTS", "myset"), 0)
	assertIntegerEq(t, cDo(t, client, "SCARD", "myset"), 0)

	// SPOP with count removing all elements at once.
	cDo(t, client, "DEL", "myset2")
	cDo(t, client, "SADD", "myset2", "x", "y", "z")
	assertIntegerEq(t, cDo(t, client, "SCARD", "myset2"), 3)
	items := unwrapArray(t, cDo(t, client, "SPOP", "myset2", "3"))
	if len(items) != 3 {
		t.Fatalf("expected 3 popped, got %d", len(items))
	}
	assertIntegerEq(t, cDo(t, client, "EXISTS", "myset2"), 0)

	// SPOP one-at-a-time until empty.
	cDo(t, client, "DEL", "myset3")
	cDo(t, client, "SADD", "myset3", "1", "2", "3", "4", "5")
	for i := 0; i < 5; i++ {
		cDo(t, client, "SPOP", "myset3")
	}
	assertIntegerEq(t, cDo(t, client, "EXISTS", "myset3"), 0)
}

// setRandomValue mirrors the Rust `set_random_value` RNG call sequence so that
// the SADD commands and the expected hashset are derived from the same stream.
func setRandomValue(rng *rand.Rand) string {
	switch rng.Intn(4) {
	case 0:
		return strconv.Itoa(rng.Intn(1999) - 999)
	case 1:
		return strconv.FormatInt(rng.Int63n(4000000001)-2000000000, 10)
	case 2:
		return strconv.FormatInt(rng.Int63n(2000000000001)-1000000000000, 10)
	default:
		n := rng.Intn(63) + 1
		var b strings.Builder
		for i := 0; i < n; i++ {
			idx := rng.Intn(52)
			if idx < 26 {
				b.WriteByte(byte('a' + idx))
			} else {
				b.WriteByte(byte('A' + idx - 26))
			}
		}
		return b.String()
	}
}

func Test_TCL_tcl_sdiff_fuzzing(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	rng := rand.New(rand.NewSource(42))

	for j := 0; j < 100; j++ {
		expected := map[string]bool{}
		numSets := rng.Intn(10) + 1
		keys := make([]string, 0, numSets)
		for i := 0; i < numSets; i++ {
			key := fmt.Sprintf("fuzzset_%d_%d{t}", j, i)
			cDo(t, client, "DEL", key)
			keys = append(keys, key)
			numElements := rng.Intn(100)
			for k := 0; k < numElements; k++ {
				ele := setRandomValue(rng)
				cDo(t, client, "SADD", key, ele)
				if i == 0 {
					expected[ele] = true
				} else {
					delete(expected, ele)
				}
			}
		}
		args := []interface{}{"SDIFF"}
		for _, k := range keys {
			args = append(args, k)
		}
		result := extractBulkStrings(t, cDo(t, client, args...))
		sort.Strings(result)
		exp := make([]string, 0, len(expected))
		for k := range expected {
			exp = append(exp, k)
		}
		sort.Strings(exp)
		assertStringsEq(t, result, exp...)
	}
}
