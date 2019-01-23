package franz

import (
	"bitbucket.org/bertrandchenal/netstring"
	"golang.org/x/net/websocket"
	"io"
	"log"
	// "time"
)

type Client struct {
	url string
	ws  *websocket.Conn
}

func NewClient(server_url string) *Client {
	// origin := "http://localhost/"
	// url := "ws://localhost:9090/ws"
	return &Client{url: server_url}
}

func (self *Client) Publish(tube string, msg []byte) []byte {
	self.Connect()
	var payload []byte
	payload, err := netstring.Encode(
		[]byte("pub"),
		[]byte(tube),
		msg,
	)
	if err != nil {
		log.Fatal("[ENCODE]",err)
	}

	if err := websocket.Message.Send(self.ws, payload); err != nil {
		log.Fatal("[SEND]", err)
	}
	websocket.Message.Receive(self.ws, &payload)
	return payload
}

func (self *Client) Ping() bool {
	self.Connect()
	payload, err := netstring.Encode(
		[]byte("ping"),
	)
	if err != nil {
		log.Fatal("[ENCODE]", err)
	}
	if err := websocket.Message.Send(self.ws, payload); err != nil {
		log.Fatal("[SEND]", err)
	}
	websocket.Message.Receive(self.ws, &payload)
	return string(payload) == "pong"
}

func (self *Client) Peers() []string {
	self.Connect()
	payload, err := netstring.Encode(
		[]byte("peers"),
	)
	if err != nil {
		log.Fatal("[ENCODE]", err)
	}
	if err := websocket.Message.Send(self.ws, payload); err != nil {
		log.Println("[SEND]", err)
		return nil
	}
	websocket.Message.Receive(self.ws, &payload)

	// "peer" message returns the list of known peers
	items, err := netstring.DecodeString(payload)

	if err != nil {
		log.Println(err)
		return nil
	}
	return items
}

func (self *Client) Subscribe(tube string) {
	self.Connect()
	var payload []byte
	payload, err := netstring.EncodeString(
		"sub",
		tube,
		"0",
		"0",
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := websocket.Message.Send(self.ws, payload); err != nil {
		log.Fatal(err)
	}
	for {
		var payload []byte
		websocket.Message.Receive(self.ws, &payload)
		if len(payload) == 0 {
			break
		}
		items, err := netstring.Decode(payload)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		log.Println("DATA", items)
	}
}

func (self *Client) Connect() {
	if self.ws != nil {
		return
	}
	ws, err := websocket.Dial(self.url, "", "http://example.com/")
	self.ws = ws
	if err != nil {
		log.Print("Failed to connect")
		return
	}

	// Close all websockets on interrupt
	// c := make(chan os.Signal, 1)
	// signal.Notify(c, os.Interrupt)
	// go func() {
	// 	<-c
	// 	self.ws.Close()
	// }()
}
