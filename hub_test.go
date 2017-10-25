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

	value := <-early_chan
	if string(value.data) != string(hello) {
		t.Error("Unexpected value:", string(value.data))
	}

	// test without tags
	offset := int64(0)
	for i := 0; i < 5; i++ {
		resp_chan := hub.Subscribe(offset)
		value := <-resp_chan
		if value.status == not_found {
			panic("NOT FOUND")
			continue
		}
		if string(value.data) != string(hello) {
			t.Error("Unexpected value:", value.data)
		}
		offset += int64(len(value.data))
	}

	// test with tags
	// for i := 0; i < 5; i++ {
	// 	var tag string
	// 	if i % 2 == 0 {
	// 		tag = "even"
	// 	} else {
	// 		tag = "odd"
	// 	}
	// 	resp_chan := hub.Subscribe(offset, tag)
	// 	value := <-resp_chan
	// 	if value.status == not_found {
	// 		panic("NOT FOUND")
	// 		continue
	// 	}
	// 	if string(value.data) != string(hello) {
	// 		t.Error("Unexpected value:", value.data)
	// 	}
	// 	offset += int64(len(value.data))
	// }

}