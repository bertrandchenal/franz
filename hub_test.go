package franz

import (
	"testing"
	"time"
)

func TestSubscribeFirst(t *testing.T) {
	cleanup()
	tube := NewTube(".", TEST_DIR)
	hub := NewHub(tube)

	early_chan := hub.Subscribe(0)
	hello := []byte("hello")

	for i := 0; i < 5; i++ {
		var tag string
		if i%2 == 0 {
			tag = "even"
		} else {
			tag = "odd"
		}
		go func() {
			time.Sleep(time.Duration(i) * time.Nanosecond)
			hub.Publish(hello, tag)
		}()
		hub.Publish(hello, tag)
	}

	msg := <-early_chan
	if string(msg.data) != string(hello) {
		t.Error("Unexpected value:", string(msg.data))
	}

	// test without tags
	offset := int64(0)
	for i := 0; i < 5; i++ {
		resp_chan := hub.Subscribe(offset)
		msg := <-resp_chan
		if msg.status == not_found {
			panic("NOT FOUND")
			continue
		}
		if string(msg.data) != string(hello) {
			t.Error("Unexpected value:", msg.data)
		}
		offset += int64(len(msg.data))
	}
}


func TestPublishFirst(t *testing.T) {
	cleanup()
	tube := NewTube(".", TEST_DIR)
	hub := NewHub(tube)

	hello := []byte("hello")
	for i := 0; i < 5; i++ {
		var tag string
		if i%2 == 0 {
			tag = "even"
		} else {
			tag = "odd"
		}
		go func() {
			time.Sleep(time.Duration(i) * time.Nanosecond)
			hub.Publish(hello, tag)
		}()
		hub.Publish(hello, tag)
	}

	// test without tags
	offset := int64(0)
	for i := 0; i < 5; i++ {
		resp_chan := hub.Subscribe(offset)
		msg := <-resp_chan
		if msg.status == not_found {
			panic("NOT FOUND")
			continue
		}
		if string(msg.data) != string(hello) {
			t.Error("Unexpected value:", msg.data)
		}
		offset += int64(len(msg.data))
	}
}
