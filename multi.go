package franz

import "sync"

type Ticket struct {
	offset   int
	sub_chan chan int
}

type Hub struct {
	pub_chan    chan int     // Incoming publication
	sub_chan    chan *Ticket // Incoming subscription
	ticket_pool []*Ticket    // Pool of ticket (subscription not yet answered)
	mutex       *sync.Mutex
	data        []int
}

func NewHub() *Hub {
	pub_chan := make(chan int)
	sub_chan := make(chan *Ticket)
	ticket_pool := make([]*Ticket, 0, 5)
	mutex := &sync.Mutex{}
	hub := &Hub{
		pub_chan:    pub_chan,
		sub_chan:    sub_chan,
		ticket_pool: ticket_pool,
		mutex:       mutex,
		data:        make([]int, 0),
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
			for _, ticket := range self.ticket_pool {
				// Answer to subscribers
				ticket.sub_chan <- self.data[ticket.offset]
			}
			// Clear pool
			self.ticket_pool = make([]*Ticket, 0, 5)
			self.mutex.Unlock()
		case ticket := <-self.sub_chan:
			if ticket.offset == len(self.data) || ticket.offset == -1 {
				// Queue tickets that reach the tail (or ask for it)
				if ticket.offset == -1 {
					ticket.offset = len(self.data)
				}
				self.mutex.Lock()
				self.ticket_pool = append(self.ticket_pool, ticket)
				self.mutex.Unlock()
			} else if ticket.offset < len(self.data) {
				// Answer directly with available data
				ticket.sub_chan <- self.data[ticket.offset]
			} else {
				// Requested offset is out of bound
				ticket.sub_chan <- -1
			}
		}
	}
}
