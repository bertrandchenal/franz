package franz

import (
	"bitbucket.org/bertrandchenal/netstring"
	"context"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/websocket"
	"io"
	"net/http"
	"strconv"
	"time"
)

var sLog = log.WithFields(log.Fields{
	"who": "Server",
})

type Server struct {
	root_path     string
	bind          string
	hubs          map[string]*Hub
	shutdown_chan chan time.Duration
	member        *Member
	http_server   *http.Server
}

func NewServer(root_path string, bind string) *Server {
	hubs := make(map[string]*Hub)
	return &Server{
		root_path:     root_path,
		bind:          bind,
		hubs:          hubs,
		shutdown_chan: make(chan time.Duration, 1),
	}
}

func (self *Server) Join(peers []string) {
	if self.member == nil {
		self.member = NewMember(self.bind, peers)
	} else {
		self.member.AddPeers(peers)
	}
}

func (self *Server) Run() {
	self.http_server = &http.Server{
		Addr:    self.bind,
		Handler: websocket.Handler(self.WSHandler),
	}

	bindLog := sLog.WithFields(log.Fields{
		"bind": self.bind,
	})
	bindLog.Info("Server started")

	if err := self.http_server.ListenAndServe(); err != http.ErrServerClosed {
		bindLog.Warn("Server stopped on error: ", err)
	} else {
		bindLog.WithFields(log.Fields{}).Info("Server stopped")
	}
}

func (self *Server) Shutdown() {
	err := self.http_server.Shutdown(context.Background())
	if err != nil {
		// Error from closing listeners, or context timeout:
		sLog.Info("HTTP server Shutdown: %v", err)
	}
	// Shutdown in flight connections
	self.shutdown_chan <- 0 // TODO parametrize timeout
}

func (self *Server) GetHub(name string) *Hub {
	// TODO should use a lock to prevent double-opening of the same hub
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
		sLog.Warn("Unable to respond to publish query:", err)
	}
}

func (self *Server) Ping(ws *websocket.Conn) {
	if err := websocket.Message.Send(ws, []byte("pong")); err != nil {
		sLog.Warn("Unable to send ping message:", err)
	}
}

func (self *Server) Peers(ws *websocket.Conn) {
	peers := []string{}
	if self.member != nil {
		for _, peer := range self.member.Peers {
			peers = append(peers, peer.Client.url)
		}
	}
	payload, err := netstring.EncodeString(peers...)
	if err != nil {
		sLog.Warn("Unable to encode message:", err)
	}
	if err := websocket.Message.Send(ws, payload); err != nil {
		sLog.Warn("Unable to respond to peer query:", err)
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
			sLog.Info("Unable to encode data", err)
			break
		}
		if err := websocket.Message.Send(ws, payload); err != nil {
			sLog.Info("Unable to send data", err)
			break
		}
		offset = msg.next_offset
	}
}

func (self *Server) WSHandler(ws *websocket.Conn) {

	go func() {
		// Add quick deadline when server closes
		nb_seconds := <-self.shutdown_chan
		// TODO SEND message warning client
		ws.SetDeadline(time.Now().Add(nb_seconds * time.Minute))
		// Pass it forward
		self.shutdown_chan <- nb_seconds
	}()

	var payload []byte
	for {
		if err := websocket.Message.Receive(ws, &payload); err != nil {
			if err != io.EOF {
				sLog.Warn("Receive error:\n\t", err)
			}
			break
		}
		items, err := netstring.Decode(payload)
		if err != nil {
			sLog.Warn("Unable to decode:", err)
			break
		}
		if len(items) == 0 {
			sLog.Warn("Empty content")
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
		case "peers":
			self.Peers(ws)
		default:
			sLog.Warn("Unknown action:", action)
		}

	}
}
