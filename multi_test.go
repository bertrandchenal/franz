package franz

import (
	"testing"
	"time"
)

func TestHub(t *testing.T) {
	hub := NewHub()

	early_chan := hub.Subscribe(-1)

	for i := 0; i < 5; i++ {
		hub.Publish(i)
		// Sleeping between each call to "force" ordering
		time.Sleep(100000)
	}

	value := <-early_chan
	if value != 0 {
		t.Error("Unexpected value:", value)
	}

	for i := 0; i < 5; i++ {
		resp_chan := hub.Subscribe(i)
		value := <-resp_chan
		if value != i {
			t.Error("Unexpected value:", value)
		}
	}
}
