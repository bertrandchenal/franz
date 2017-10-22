package franz

import (
	"sync"
)

const (
	success   = iota
	not_found = iota
)

type Message struct {
	data   []byte
	status int
	tags   []string
}

type Ticket struct {
	offset    int64
	resp_chan chan *Message
	tags      []string
}

type Hub struct {
	pub_chan    chan *Message       // Incoming publication
	sub_chan    chan *Ticket        // Ticket waiting for the next publication
	ticket_pool map[int64][]*Ticket // Pool of ticket (subscription not yet answered)
	mutex       *sync.Mutex
	tube        *Tube
}

func NewHub(tube *Tube) *Hub {
	hub := &Hub{
		pub_chan:    make(chan *Message, 1), // REMOVEME (the 10)
		sub_chan:    make(chan *Ticket, 1),
		ticket_pool: make(map[int64][]*Ticket),
		mutex:       &sync.Mutex{},
		tube:        tube,
	}
	// Start schedulers
	for i := 0; i < 3; i++ {
		go hub.Scheduler()
	}
	return hub
}

func NewTicket(offset int64, tags []string) *Ticket {
	resp_chan := make(chan *Message, 1)
	return &Ticket{offset, resp_chan, tags}
}

func (self *Hub) Publish(data []byte, tags ...string) {
	msg := Message{
		data: data,
		tags: tags,
	}
	self.pub_chan <- &msg
}

func (self *Hub) Subscribe(offset int64, tags ...string) chan *Message {
	ticket := NewTicket(offset, tags)
	self.sub_chan <- ticket
	return ticket.resp_chan
}

func (self *Hub) Broadcast(ticket *Ticket) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// Add ticket to pool
	if ticket != nil {
		self.ticket_pool[ticket.offset] = append(self.ticket_pool[ticket.offset], ticket)
	}

	// Loop on all ticket and try to answer them
	for offset, tickets := range self.ticket_pool {
		// Early return if offset is too large
		if offset >= self.tube.Len {
			continue
		}
		// Answer to subscribers
		data, err := self.tube.Read(offset)
		if err != nil {
			panic(err)
		}
		for _, ticket := range tickets {
			ticket.resp_chan <- &Message{data: data, status: success}
		}
		// Clear pool
		delete(self.ticket_pool, offset)
	}
}

func (self *Hub) Scheduler() {
	for {
		select {
		case msg := <-self.pub_chan:
			self.tube.Append(msg.data)
			self.Broadcast(nil)

		case ticket := <-self.sub_chan:
			if ticket.offset > self.tube.Len {
				// Requested offset is out of bound
				ticket.resp_chan <- &Message{data: nil, status: not_found}
				break
			}
			// Negative offset means wait for next msg
			if ticket.offset < 0 {
				ticket.offset = self.tube.Len
			}
			// Try to answer directly with available data
			self.Broadcast(ticket)
		}
	}
}
