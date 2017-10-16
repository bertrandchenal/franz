package franz

import "sync"

type Ticket struct {
	offset   int
	sub_chan chan Message
}

type Message struct {
	data []byte
	tags []string
}

type Hub struct {
	pub_chan    chan Message     // Incoming publication
	sub_chan    chan *Ticket // Incoming subscription
	ticket_pool []*Ticket    // Pool of ticket (subscription not yet answered)
	mutex       *sync.Mutex
	tube        *Tube
}

func NewHub(tube *Tube) *Hub {
	pub_chan := make(chan Message)
	sub_chan := make(chan *Ticket)
	ticket_pool := make([]*Ticket, 0, 5)
	mutex := &sync.Mutex{}
	hub := &Hub{
		pub_chan:    pub_chan,
		sub_chan:    sub_chan,
		ticket_pool: ticket_pool,
		mutex:       mutex,
		tube:        tube
	}
	// Start a bunch of workers
	for i := 0; i < 5; i++ {
		go hub.Scheduler()
	}
	return hub
}

func (self *Hub) Publish(msg) {
	self.pub_chan <- msg
}

func (self *Hub) Subscribe(offset int) chan Message {
	ticket_chan := make(chan Message)
	ticket := &Ticket{offset, ticket_chan}
	self.sub_chan <- ticket
	return ticket_chan
}

func (self *Hub) Scheduler() {
	for {
		select {
		case msg := <-self.pub_chan:
			self.mutex.Lock()
			// Append data
			self.tube.append(msg.data, ...msg.tags)
			for _, ticket := range self.ticket_pool {
				// Answer to subscribers
				ticket.sub_chan <- self.tube.Read(ticket.offset)
			}
			// Clear pool
			self.ticket_pool = make([]*Ticket, 0, 5)
			self.mutex.Unlock()
		case ticket := <-self.sub_chan:
			if ticket.offset == self.Tube.Len || ticket.offset == -1 {
				// Queue tickets that reached the tail (or ask for it)
				if ticket.offset == -1 {
					ticket.offset = self.Tube.Len
				}
				self.mutex.Lock()
				self.ticket_pool = append(self.ticket_pool, ticket)
				self.mutex.Unlock()
			} else if ticket.offset < len(self.data) {
				// Answer directly with available data
				ticket.sub_chan <- self.tube.Get(ticket.offset)
			} else {
				// Requested offset is out of bound
				ticket.sub_chan <- -1
			}
		}
	}
}
