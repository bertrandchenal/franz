package franz

import (
	"github.com/bertrandchenal/netstring"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/websocket"
	"io"
	"time"
)

type Client struct {
	url string
	ws  *websocket.Conn
	log *log.Entry
}

func NewClient(server_url string) *Client {
	log := NewLogger("client")
	return &Client{url: server_url, log: log}
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
		self.log.Fatal("Unable to encode publish message:\n\t", err)
	}

	if err := websocket.Message.Send(self.ws, payload); err != nil {
		self.log.Fatal("Unable to send publish message:\n\t", err)
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
		self.log.Fatal("Unable to encode ping message:\n\t", err)
	}
	if err := websocket.Message.Send(self.ws, payload); err != nil {
		self.log.Fatal("Unable to send ping message:\n\t", err)
	}
	websocket.Message.Receive(self.ws, &payload)
	return string(payload) == "pong"
}

func (self *Client) GetPeers() []string {
	self.Connect()
	payload, err := netstring.Encode(
		[]byte("getpeers"),
	)
	if err != nil {
		self.log.Fatal("Unable to encode peer message:\n\t", err)
	}
	if err := websocket.Message.Send(self.ws, payload); err != nil {
		self.log.Println("Unable to send peer message:\n\t", err)
		return nil
	}
	websocket.Message.Receive(self.ws, &payload)

	// "peer" message returns the list of known peers
	items, err := netstring.DecodeString(payload)

	if err != nil {
		self.log.Println(err)
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
		self.log.Fatal(err)
	}
	if err := websocket.Message.Send(self.ws, payload); err != nil {
		self.log.Fatal(err)
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
			self.log.Fatal(err)
		}
		self.log.Println("DATA", items)
	}
}

func (self *Client) Connect() {
	if self.ws != nil {
		return
	}
	for {
		ws, err := websocket.Dial(self.url, "", "http://example.com/")
		if err == nil {
			self.ws = ws
			return
		}
		time.Sleep(5e8)
	}
	self.log.Print("Failed to connect")

	// Close all websockets on interrupt
	// c := make(chan os.Signal, 1)
	// signal.Notify(c, os.Interrupt)
	// go func() {
	// 	<-c
	// 	self.ws.Close()
	// }()
}
