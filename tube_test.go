package franz

import (
	"testing"
)

func TestInit(t *testing.T) {
	tube := NewTube("test-dir")
	if len(tube.buckets) > 0 {
		t.Error("data dir not clean")
	}
}
