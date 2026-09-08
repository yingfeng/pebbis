package integration

import (
	"testing"
)

// TestHll is intentionally skipped: redistore does not implement the HyperLogLog
// family (PFADD / PFCOUNT / PFMERGE), so this differential test cannot pass.
func TestHll(t *testing.T) {
	t.Skip("HyperLogLog is not supported by redistore")
}
