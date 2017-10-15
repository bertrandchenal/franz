package franz

import (
	"testing"
)

func TestHub(t *testing.T) {
	hub := NewHub()

	early_chan := hub.Subscribe(-1)

	for i := 0; i < 5; i++ {
		hub.Publish(i)
	}

	value := <-early_chan
	println(value)

	for i := 0; i < 5; i++ {
		resp_chan := hub.Subscribe(i)
		value := <-resp_chan
		println("got", value)
	}
}
