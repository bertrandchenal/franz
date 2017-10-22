package franz

import (
	"testing"
	"time"
)

func TestHub(t *testing.T) {
	cleanup()
	tube := NewTube(".", TEST_DIR)
	hub := NewHub(tube)

	early_chan := hub.Subscribe(0)
	hello := []byte("hello")

	for i := 0; i < 5; i++ {
		hub.Publish(Message{hello, success})
		// Sleeping between each call to "force" ordering
		time.Sleep(100000)
	}

	value := <-early_chan
	if string(value.data) != string(hello) {
		t.Error("Unexpected value:", value)
	}

	offset := int64(0)
	for i := 0; i < 5; i++ {
		resp_chan := hub.Subscribe(offset)
		value := <-resp_chan
		if value.status == not_found {
			continue
		}
		if string(value.data) != string(hello) {
			t.Error("Unexpected value:", value.data)
		}
		offset += int64(len(value.data))
	}
}
