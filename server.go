package franz

import (
	"bitbucket.org/bertrandchenal/netstring"
	"golang.org/x/net/websocket"
	"io"
	"log"
	"net/http"
	"strconv"
)

type Server struct {
	root_path string
	bind   string
	hubs      map[string]*Hub
	member *Member
}

func NewServer(root_path string, bind string) *Server {
	hubs := make(map[string]*Hub)
	return &Server{root_path, bind, hubs, nil}
}

func (self *Server) Join(peers []string) {
	if self.member == nil {
		self.member = NewMember(self.bind, peers)
	} else {
		self.member.AddPeers(peers)
	}
}

func (self *Server) Run() {
	http.Handle("/ws", websocket.Handler(self.WSHandler))
	log.Println("Server started")
	if err := http.ListenAndServe(self.bind, nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}

func (self *Server) GetHub(name string) *Hub {
	// TODO sanitize name!
	hub, found := self.hubs[name]
	if !found {
		tube := NewTube(self.root_path, name)
		hub = NewHub(tube)
		self.hubs[name] = hub
	}
	return hub
}

func (self *Server) Publish(ws *websocket.Conn, args [][]byte) {
	name := string(args[0])
	data := args[1]
	hub := self.GetHub(name)
	tags := []string{}
	for _, tag := range args[2:] {
		tags = append(tags, string(tag))
	}

	hub.Publish(data, tags...)
	if err := websocket.Message.Send(ws, []byte("OK")); err != nil {
		log.Println("[SEND]", err)
	}
}

func (self *Server) Ping(ws *websocket.Conn) {
	if err := websocket.Message.Send(ws, []byte("pong")); err != nil {
		log.Println("[PING]", err)
	}
}

func (self *Server) Subscribe(ws *websocket.Conn, args [][]byte) {
	name := string(args[0])
	hub := self.GetHub(name)
	offset := int64(0)
	timestamp := int64(0)
	tags := []string{}
	// Extract offset
	if len(args) > 1 {
		i, err := strconv.Atoi(string(args[1]))
		check(err)
		offset = int64(i)
	}
	// Extract timestamp
	if len(args) > 2 {
		i, err := strconv.Atoi(string(args[2]))
		check(err)
		timestamp = int64(i)
	}
	// Extract tags
	for _, tag := range args[3:] {
		tags = append(tags, string(tag))
	}
	for {
		resp_chan := hub.Subscribe(offset, timestamp, tags...)
		msg := <-resp_chan
		if msg.status == not_found {
			break
		}
		next_offset := strconv.FormatInt(msg.next_offset, 10)
		payload, err := netstring.Encode(msg.data, []byte(next_offset))
		if err != nil {
			log.Println("[ENCODE SEND]", err)
			break
		}
		if err := websocket.Message.Send(ws, payload); err != nil {
			log.Println("[MSG SEND]", err)
			break
		}
		offset = msg.next_offset
	}
}

func (self *Server) WSHandler(ws *websocket.Conn) {
	for {
		var payload []byte
		if err := websocket.Message.Receive(ws, &payload); err != nil {
			if err != io.EOF {
				log.Println("[RECEIVE]", err)
			}
			break
		}
		items, err := netstring.Decode(payload)
		if err != nil {
			log.Println("[DECODE]", err)
			break
		}
		if len(items) == 0 {
			log.Println("[EMPTY]")
			continue
		}
		action := string(items[0])
		switch action {
		case "pub":
			self.Publish(ws, items[1:])
		case "sub":
			self.Subscribe(ws, items[1:])
		case "ping":
			self.Ping(ws)
		default:
			log.Println("[UNKNOWN]", action)
		}
	}
}
