package franz

import "time"


func Waiter(sub chan int, wait_signal chan int, sub_id, value int) {
	// if unsuscribe was done too late
    // defer func() {
    //     if r := recover(); r != nil {
    //         log.Printf("Panic: %v", r)
    //     }
    // }()

	select {
	case sub <- value + sub_id:
		return
	case <- time.After(500 * time.Millisecond):
		println("waiter timeout!", sub_id)
		wait_signal <- sub_id
		return
	}
}

func Publisher(pub_signal chan struct{}, subscribers map[int]chan int) {
	wait_signal := make(chan int)
	tick := time.Tick(100 * time.Millisecond)
	step := 0
	for {
		select {
		case died_sub := <- wait_signal:
			println("DIED", died_sub)
			delete(subscribers, died_sub)
		case <- tick:
			for i, s := range(subscribers) {
				println("GO", i)
				go Waiter(s, wait_signal, i, step)
			}
			step += 1
		case <- pub_signal:
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


