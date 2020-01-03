package franz

import (
	"sync"
)

const (
	success   = iota
	not_found = iota
)

type Message struct {
	data []byte
	tags []string
}

type Response struct {
	data        []byte
	status      int
	next_offset int64
}

type Ticket struct {
	offset    int64
	timestamp int64
	resp_chan chan *Response
	tags      []string
}

type Hub struct {
	pub_chan    chan *Message // Incoming publication
	sub_chan    chan *Ticket  // Ticket waiting for the next publication
	ticket_pool []*Ticket     // Pool of ticket (subscription not yet answered)
	mutex       *sync.Mutex
	tube        *Tube
}

func NewHub(tube *Tube) *Hub {
	hub := &Hub{
		pub_chan:    make(chan *Message, 0),
		sub_chan:    make(chan *Ticket, 0),
		ticket_pool: make([]*Ticket, 0, 1),
		mutex:       &sync.Mutex{},
		tube:        tube, // TODO list or map of tubes
	}
	// Start scheduler
	go hub.Scheduler()
	return hub
}

func NewTicket(offset int64, timestamp int64, tags []string) *Ticket {
	resp_chan := make(chan *Response, 1)
	return &Ticket{offset, timestamp, resp_chan, tags}
}

func (self *Hub) Publish(data []byte, tags ...string) {
	// Create a message and add it to hub's pub chan
	msg := Message{
		data: data,
		tags: tags,
	}
	self.pub_chan <- &msg
}

func (self *Hub) Subscribe(offset int64, timestamp int64, tags ...string) chan *Response {
	// Create a ticket, add it to hub's subscribtion chan, returns
	// ticket's response chan
	ticket := NewTicket(offset, timestamp, tags)
	self.sub_chan <- ticket
	return ticket.resp_chan
}

func (self *Hub) Broadcast(new_ticket *Ticket) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// Add ticket to pool
	if new_ticket != nil {
		self.ticket_pool = append(self.ticket_pool, new_ticket)
	}

	// Loop on all ticket and try to answer them
	new_pool := make([]*Ticket, 0, 5)
	for _, ticket := range self.ticket_pool {
		// Early return if offset is too large
		if ticket.offset >= self.tube.Len { // TODO implement MaxLen (loop across tubes)
			new_pool = append(new_pool, ticket)
			continue
		}
		// Answer to subscribers
		next_offset, data, err := self.tube.Read(ticket.offset, // TODO implemennt self.Read that will loop on tubes
			ticket.timestamp, ticket.tags...)
		if err != nil {
			panic(err)
		} else if data == nil {
			ticket.resp_chan <- &Response{
				data:        nil,
				next_offset: next_offset,
				status:      not_found,
			}
		} else {
			ticket.resp_chan <- &Response{
				data:        data,
				next_offset: next_offset,
				status:      success,
			}
		}
		// Clear pool
		self.ticket_pool = new_pool
	}
}

func (self *Hub) Scheduler() {
	for {
		select {
		case msg := <-self.pub_chan:
			self.tube.Append(msg.data, msg.tags...)
			self.Broadcast(nil)

		case ticket := <-self.sub_chan:
			if ticket.offset > self.tube.Len {
				// Requested offset is out of bound
				ticket.resp_chan <- &Response{data: nil, status: not_found}
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
