package franz

import (
	"testing"
)

func TestHub(t *testing.T) {
	hub := NewHub()

	for i := 0; i < 5; i++ {
		hub.Publish(i)
	}

	for i := 0; i < 5; i++ {
		resp_chan := hub.Subscribe(i)
		value := <-resp_chan
		println("got", value)
	}
}
