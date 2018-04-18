package franz

import (
	"github.com/yawn/netstring"
	"golang.org/x/net/websocket"
	"log"
	"net/http"
	"strconv"
)

type Server struct {
	root_path string
	address   string
	hubs      map[string]*Hub
}

func NewServer(root_path *string, address *string) *Server {
	hubs := make(map[string]*Hub)
	return &Server{*root_path, *address, hubs}
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
			tags := make([]string, len(items)-3)
			for item := range items[3:] {
				tags = append(tags, string(item))
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
			if len(items) > 2 {
				i, err := strconv.Atoi(string(items[2]))
				check(err)
				offset = int64(i)
			}
			for {
				resp_chan := hub.Subscribe(offset)
				msg := <-resp_chan
				if msg.status == not_found {
					if err := websocket.Message.Send(ws, []byte("KO")); err != nil {
						log.Println("[KO SEND]", err)
					}
					break
				}

				if err := websocket.Message.Send(ws, msg.data); err != nil {
					log.Println("[MSG SEND]", err)
					break
				}
				offset += int64(len(msg.data))
			}
		}

	}
}
