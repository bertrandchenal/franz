package franz

import (
	"time"
	"testing"
)

func TestPubSub(t *testing.T) {

	pub_signal := make(chan struct{})
	sub_a := make(chan int)
	sub_b := make(chan int)
	subs := map[int]chan int{
		1: sub_a,
		2: sub_b,
	}
	go Publisher(pub_signal, subs)
	go Subscriber(sub_a)
	go Subscriber(sub_b)


	time.Sleep(3e9)
	close(pub_signal)
	time.Sleep(1e9)
	println("main done")
}
