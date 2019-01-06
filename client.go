package franz

import (
	"bitbucket.org/bertrandchenal/netstring"
	"golang.org/x/net/websocket"
	"io"
	"log"
	"os"
	"os/signal"
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

func (self *Client) Publish(tube string, msg []byte) {
	self.Connect()
	var payload []byte
	payload, err := netstring.Encode(
		[]byte("pub"),
		[]byte(tube),
		msg,
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := websocket.Message.Send(self.ws, payload); err != nil {
		log.Fatal(err)
	}
	// websocket.Message.Receive(self.ws, &payload)
	// println(string(payload))
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
		items, err := netstring.Decode(payload)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if len(payload) == 0 {
			break
		}
		println(string(items[0]))
	}
}

func (self *Client) Connect() {
	ws, err := websocket.Dial(self.url, "", "http://example.com/")
	self.ws = ws
	if err != nil {
		log.Fatal("[DIAL]", err)
	}

	// Close websocket on interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		self.ws.Close()
	}()
}
