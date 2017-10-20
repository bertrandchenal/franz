package franz

import (
	"testing"
	"time"
)

func TestHub(t *testing.T) {
	tube := NewTube(".", TEST_DIR)
	hub := NewHub(tube)

	early_chan := hub.Subscribe(0)
	hello := []byte("hello")

	for i := 0; i < 5; i++ {
		hub.Publish(Message{hello})
		// Sleeping between each call to "force" ordering
		time.Sleep(100000)
	}

	value := <-early_chan
	if string(value.data) != string(hello) {
		t.Error("Unexpected value:", value)
	}

	for i := 0; i < 5; i++ {
		resp_chan := hub.Subscribe(0)
		value := <-resp_chan
		if string(value.data) != string(hello) {
			t.Error("Unexpected value:", value)
		}
	}
}
