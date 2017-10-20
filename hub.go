package franz

import "sync"

type Ticket struct {
	offset   int64
	sub_chan chan Message
}

type Message struct {
	data []byte
	// tags []string // TODO
}

type Hub struct {
	pub_chan    chan Message // Incoming publication
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
		tube:        tube,
	}
	// Start a bunch of workers
	for i := 0; i < 5; i++ {
		go hub.Scheduler()
	}
	return hub
}

func (self *Hub) Publish(msg Message) {
	self.pub_chan <- msg
}

func (self *Hub) Subscribe(offset int64) chan Message {
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
			self.tube.Append(msg.data)
			for _, ticket := range self.ticket_pool {
				// Answer to subscribers
				data, err := self.tube.Read(ticket.offset)
				if err != nil {
					panic(err)
				}
				ticket.sub_chan <- Message{data}
			}
			// Clear pool
			self.ticket_pool = make([]*Ticket, 0, 5)
			self.mutex.Unlock()
		case ticket := <-self.sub_chan:
			if ticket.offset == self.tube.Len || ticket.offset == -1 {
				// Queue tickets that reached the tail (or ask for it)
				if ticket.offset == -1 {
					ticket.offset = self.tube.Len
				}
				self.mutex.Lock()
				self.ticket_pool = append(self.ticket_pool, ticket)
				self.mutex.Unlock()
			} else if ticket.offset < self.tube.Len {
				// Answer directly with available data
				data, err := self.tube.Read(ticket.offset)
				if err != nil {
					panic(err)
				}
				ticket.sub_chan <- Message{data}
			} else {
				// Requested offset is out of bound
				ticket.sub_chan <- Message{}
			}
		}
	}
}
