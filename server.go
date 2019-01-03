package franz

import (
	"golang.org/x/net/websocket"
	"bitbucket.org/bertrandchenal/netstring"
	"log"
	"net/http"
	"strconv"
)

type Server struct {
	root_path string
	address   string
	hubs      map[string]*Hub
}

func NewServer(root_path string, address string) *Server {
	hubs := make(map[string]*Hub)
	return &Server{root_path, address, hubs}
}

func (self *Server) Run() {
	http.Handle("/ws", websocket.Handler(self.WSHandler))
	log.Println("Server started")
	if err := http.ListenAndServe(self.address, nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}

func (self *Server) GetHub(name string) *Hub {
	hub, found := self.hubs[name]
	if !found {
		tube := NewTube(self.root_path, name)
		hub = NewHub(tube)
		self.hubs[name] = hub
	}
	return hub
}

func (self *Server) WSHandler(ws *websocket.Conn) {
	for {
		var reply []byte
		if err := websocket.Message.Receive(ws, &reply); err != nil {
			log.Println("[RECV]", err)
			break
		}

		items, err := netstring.Decode(reply)
		if err != nil {
			log.Println("[DECO]", err)
			break
		}

		action := string(items[0])
		switch action {
		case "publish":
			name := string(items[1])
			data := items[2]
			hub := self.GetHub(name)
			tags := make([]string, 0)
			for pos := 3; pos < len(items); pos++ {
				tags = append(tags, string(items[pos]))
			}

			hub.Publish(data, tags...)
			if err := websocket.Message.Send(ws, []byte("OK")); err != nil {
				log.Println("[SEND]", err)
				break
			}

		case "subscribe":
			name := string(items[1])
			hub := self.GetHub(name)
			offset := int64(0)
			timestamp := int64(0)
			tags := make([]string, 0)
			// Extract offset
			if len(items) > 2 {
				i, err := strconv.Atoi(string(items[2]))
				check(err)
				offset = int64(i)
			}
			// Extract timestamp
			if len(items) > 3 {
				i, err := strconv.Atoi(string(items[3]))
				check(err)
				timestamp = int64(i)
			}
			// Extract tags
			for pos := 4; pos < len(items); pos++ {
				tags = append(tags, string(items[pos]))
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
					log.Println("[MSG SEND]", err)
					break
				}
				if err := websocket.Message.Send(ws, payload); err != nil {
					log.Println("[MSG SEND]", err)
					break
				}
				offset = msg.next_offset
			}
		}

	}
}
