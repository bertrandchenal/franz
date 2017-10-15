package franz

import "sync"

type Ticket struct {
	offset   int
	sub_chan chan int
}

type Hub struct {
	pub_chan chan int          // Incoming publication
	sub_chan chan *Ticket      // Incoming subscription
	sub_pool map[int][]*Ticket // Pool of ticket (subscription not yet answered)
	mutex    *sync.Mutex
	data     []int
}

func NewHub() *Hub {
	pub_chan := make(chan int)
	sub_chan := make(chan *Ticket)
	sub_pool := make(map[int][]*Ticket)
	mutex := &sync.Mutex{}
	hub := &Hub{
		pub_chan: pub_chan,
		sub_chan: sub_chan,
		sub_pool: sub_pool,
		mutex:    mutex,
		data:     make([]int, 0),
	}
	// Start a bunch of workers
	for i := 0; i < 5; i++ {
		go hub.Scheduler()
	}
	return hub
}

func (self *Hub) Publish(value int) {
	self.pub_chan <- value
}

func (self *Hub) Subscribe(offset int) chan int {
	ticket_chan := make(chan int)
	ticket := &Ticket{offset, ticket_chan}
	self.sub_chan <- ticket
	return ticket_chan
}

func (self *Hub) Scheduler() {
	for {
		select {
		case value := <-self.pub_chan:
			self.mutex.Lock()
			// Append data
			self.data = append(self.data, value)
			for pos, tickets := range self.sub_pool {
				for _, ticket := range tickets {
					// Answer to subscribers
					ticket.sub_chan <- self.data[ticket.offset]
				}
				// Clear pool
				delete(self.sub_pool, pos)
			}
			self.mutex.Unlock()
		case ticket := <-self.sub_chan:
			// Try to answer ticket if possible, if not queue it in the pool
			if ticket.offset < len(self.data) {
				ticket.sub_chan <- self.data[ticket.offset]
			} else if ticket.offset > len(self.data) {
				ticket.sub_chan <- -1
			} else {
				self.mutex.Lock()
				self.sub_pool[ticket.offset] = append(self.sub_pool[ticket.offset], ticket)
				self.mutex.Unlock()
			}
		}
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
