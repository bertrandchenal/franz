package main

import "time"

func Waiter(sub chan int, value int) {
	// if unsuscribe was done too late
    defer func() {
        if r := recover(); r != nil {
            println("Recovered in f", r)
        }
    }()

	select {
	case sub <- value:
		return
	case <- time.After(500 * time.Millisecond):
		println("waiter timeout (TODO unsubscribe)!")
		// IDEA, either publisher pass the signal chan to the waiters (and let them send an signal to unsubscribe the subsciber or use a lock that is used around the subsciber list
		close(sub)
		return
	}
}

func Publisher(signal chan struct{}, subscribers []chan int) {
	tick := time.Tick(100 * time.Millisecond)
	step := 0
	for {
		select {
		case <- tick:
			for i, s := range(subscribers) {
				go Waiter(s, i + step)
			}
			step += 1
		case <-signal:
			println("pub done")
			return
		}
	}
}


func Subscriber(in chan int) {
	for i:= range(in) {
		time.Sleep(time.Duration(i * 100) * time.Millisecond)
		println(i)
	}
	println("sub done")
}


func main() {
	signal := make(chan struct{})
	sub_a := make(chan int)
	sub_b := make(chan int)
	subs := []chan int{sub_a, sub_b}
	go Publisher(signal, subs)
	go Subscriber(sub_a)
	go Subscriber(sub_b)


	time.Sleep(3e9)
	close(signal)
	time.Sleep(1e9)
	println("main done")
}
