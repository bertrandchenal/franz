package franz

import "time"

func Waiter(sub chan int, wait_signal chan int, sub_id, value int) {
	// if unsuscribe was done too late
    defer func() {
        if r := recover(); r != nil {
            println("Recovered in f", r, sub_id)
        }
    }()

	select {
	case sub <- value + sub_id:
		return
	case <- time.After(500 * time.Millisecond):
		println("waiter timeout!")
		wait_signal <- sub_id
		close(sub)
		return
	}
}

func Publisher(pub_signal chan struct{}, subscribers []chan int) {
	wait_signal := make(chan int)
	tick := time.Tick(100 * time.Millisecond)
	step := 0
	for {
		select {
		case died_sub := <- wait_signal:
			println("TODO", died_sub)
		case <- tick:
			for i, s := range(subscribers) {
				go Waiter(s, wait_signal, i, step)
			}
			step += 1
		case <-pub_signal:
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


// func main() {
// 	pub_signal := make(chan struct{})
// 	sub_a := make(chan int)
// 	sub_b := make(chan int)
// 	subs := []chan int{sub_a, sub_b}
// 	go Publisher(pub_signal, subs)
// 	go Subscriber(sub_a)
// 	go Subscriber(sub_b)


// 	time.Sleep(3e9)
// 	close(pub_signal)
// 	time.Sleep(1e9)
// 	println("main done")
// }
