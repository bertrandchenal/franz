package franz

import "time"

type Ticket {
	offset int
	sub_chan chan int
}

type Tube struct {
	pub_chan chan int
	sub_chan chan int
	sub_pool map[int]Ticket
	data []int
}


func NewHub() *Hub {
	pub_chan := make(chan int)
	sub_chan := make(map[int][]chan int)
	hub := &Hub{
		pub_chan: pub_chan,
		sub_chan: sub_chan,
		data: 0,
	}
	// Start a bunch of workers
	go hub.Scheduler()
	go hub.Scheduler()
	go hub.Scheduler()
	return hub
}

func (self *Tube) Publish(value int) {
	self.pub_chan <- value
}

func (self *Tube) Subscribe(offset int, sub_chan chan int) {
	ticket = &Ticket{offset, sub_chan}
	self.sub_chan <- ticket
}


func (self *Tube) Scheduler() {
	for {
		case value <- self.pub_chan:
		//TODO append value to self.data, and trigger all chan in
		//self.sub_pool
		case ticket <- self.sub_chan:
		//TODO trigger ticket.sub_chan if ticket.offset <
		//len(self.data), send error in ticke.sub_chan if
		//ticket.offest is > and add ticket to pool if ticket.offset =
		//len(self.data)
	}
}


// func Waiter(sub chan int, wait_signal chan int, sub_id, value int) {
// 	// if unsuscribe was done too late
//     // defer func() {
//     //     if r := recover(); r != nil {
//     //         log.Printf("Panic: %v", r)
//     //     }
//     // }()

// 	select {
// 	case sub <- value + sub_id:
// 		return
// 	case <- time.After(500 * time.Millisecond):
// 		println("waiter timeout!", sub_id)
// 		wait_signal <- sub_id
// 		return
// 	}
// }

// func Publisher(pub_signal chan struct{}, subscribers map[int]chan int) {
// 	wait_signal := make(chan int)
// 	tick := time.Tick(100 * time.Millisecond)
// 	step := 0
// 	for {
// 		select {
// 		case died_sub := <- wait_signal:
// 			println("DIED", died_sub)
// 			delete(subscribers, died_sub)
// 		case <- tick:
// 			for i, s := range(subscribers) {
// 				println("GO", i)
// 				go Waiter(s, wait_signal, i, step)
// 			}
// 			step += 1
// 		case <- pub_signal:
// 			println("pub done")
// 			return
// 		}
// 	}
// }


// func Subscriber(in chan int) {
// 	for i:= range(in) {
// 		time.Sleep(time.Duration(i * 100) * time.Millisecond)
// 		println(i)
// 	}
// 	println("sub done")
// }


