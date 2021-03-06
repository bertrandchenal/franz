package franz

import (
	"github.com/bertrandchenal/netstring"
	"context"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/websocket"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

type Server struct {
	root_path     string
	bind          string
	hubs          map[string]*Hub
	shutdown_chan chan time.Duration
	member        *Member
	http_server   *http.Server
	log           *logrus.Entry
}

func NewServer(root_path string, bind string) *Server {
	hubs := make(map[string]*Hub)

	return &Server{
		root_path:     root_path,
		bind:          bind,
		hubs:          hubs,
		shutdown_chan: make(chan time.Duration, 1),
		log:           NewLogger("server"),
	}
}

func (self *Server) Join(peers []string) {
	if self.member == nil {
		self.member = NewMember(self.bind, peers)
	} else {
		self.member.AddPeers(peers...)
	}
}

func (self *Server) Run() {
	self.http_server = &http.Server{
		Addr:    self.bind,
		Handler: websocket.Handler(self.WSHandler),
	}

	bindLog := self.log.WithField("bind", self.bind)
	bindLog.Info("Server started")

	if err := self.http_server.ListenAndServe(); err != http.ErrServerClosed {
		bindLog.Warn("Server stopped on error: ", err)
	} else {
		bindLog.Info("Server stopped")
	}
}

func (self *Server) Shutdown() {
	err := self.http_server.Shutdown(context.Background())
	if err != nil {
		// Error from closing listeners, or context timeout:
		self.log.Info("HTTP server Shutdown:", err)
	}
	// Shutdown in flight connections
	self.shutdown_chan <- 0 // TODO parametrize timeout
}

func (self *Server) GetHub(name string) *Hub {
	// Validating version of self.getHub
	match, _ := regexp.MatchString("^[a-z]+$", name)
	if !match {
		return nil // TODO return error
	}
	return self.getHub(name)
}

func (self *Server) getHub(name string) *Hub {
	// TODO should use a lock to prevent double-opening of the same hub
	hub, found := self.hubs[name]
	if !found {
		tube := NewTube(self.root_path, name)
		hub = NewHub(tube)
		self.hubs[name] = hub
	}
	return hub
}

func (self *Server) Publish(ws *websocket.Conn, args [][]byte) {
	// First item in args is tube name, second is payload, all other
	// args are tags
	if self.member != nil {
		peer := self.member.FindPeer(args[0])
		if peer.bind != self.bind {
			self.Forward(ws, args, peer)
			return
		}
	}

	tubeName := string(args[0])
	data := args[1]
	hub := self.GetHub(tubeName)
	tags := []string{}
	for _, tag := range args[2:] {
		tags = append(tags, string(tag))
	}
	if hub.tube.Len == 0 && tubeName[0] != '_' {
		// Advertise first publication
		logHub := self.getHub("_log")
		data := []byte("New tube!")
		logHub.Publish(data)
	}

	hub.Publish(data, tags...)
	if err := websocket.Message.Send(ws, []byte("OK")); err != nil {
		self.log.Warn("Unable to respond to publish query:", err)
	}
}

func (self *Server) Forward(ws *websocket.Conn, args [][]byte, peer *Peer) {
	// Forward message contained in args to peer. If for any reason
	// peer is not available (or refuse the message) the message is
	// temporarily saved in a local tube
}

func (self *Server) Ping(ws *websocket.Conn) {
	if err := websocket.Message.Send(ws, []byte("pong")); err != nil {
		self.log.Warn("Unable to send ping message:", err)
	}
}

func (self *Server) GetPeers(ws *websocket.Conn) {
	peer_info := []string{}
	now := time.Now().Unix()
	if self.member != nil {
		for _, peer := range self.member.peers {
			delta := now - peer.lastSeen
			lastSeen := strconv.FormatInt(delta, 10)
			peer_info = append(peer_info, peer.bind, lastSeen)
		}
	}
	payload, err := netstring.EncodeString(peer_info...)
	if err != nil {
		self.log.Warn("Unable to encode message:", err)
		return
	}
	if err := websocket.Message.Send(ws, payload); err != nil {
		self.log.Warn("Unable to respond to peer query:", err)
		return
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
			self.log.Info("Unable to encode data", err)
			break
		}
		if err := websocket.Message.Send(ws, payload); err != nil {
			self.log.Info("Unable to send data", err)
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
		ws.SetDeadline(time.Now().Add(nb_seconds * time.Second))
		// Pass it forward
		self.shutdown_chan <- nb_seconds
	}()

	var payload []byte
	for {
		if err := websocket.Message.Receive(ws, &payload); err != nil {
			if err != io.EOF {
				self.log.Warn("Receive error:\n\t", err)
			}
			break
		}
		items, err := netstring.Decode(payload)
		if err != nil {
			self.log.Warn("Unable to decode:", err)
			break
		}
		if len(items) == 0 {
			self.log.Warn("Empty content")
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
		case "getpeers":
			self.GetPeers(ws)
		default:
			self.log.Warn("Unknown action:", action)
		}

	}
}
