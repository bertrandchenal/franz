package franz

import (
	"strconv"
	"time"
)

const (
	UP = iota
	DOWN
)

type Status int

type Peer struct {
	bind     string
	client   *Client
	status   Status
	lastSeen int64
	member   *Member
	quitChan chan bool
}
type Member struct {
	bind  string
	peers map[string]*Peer
}

func NewMember(bind string, peers []string) *Member {
	member := Member{
		bind:  bind,
		peers: make(map[string]*Peer),
	}
	member.AddPeers(peers...)
	return &member
}

func (self *Member) AddPeers(binds ...string) {
	for _, bind := range binds {
		peer := NewPeer(bind, self)
		self.peers[bind] = peer
		go peer.backgroundRefresh()
	}
}

func (self *Member) RemovePeer(bind string) {
	self.peers[bind].quitChan <- true
}

func NewPeer(bind string, member *Member) *Peer {
	client := NewClient("ws://" + bind + "/ws")
	peer := Peer{
		bind:     bind,
		client:   client,
		status:   UP,
		member:   member,
		quitChan: make(chan bool, 1),
	}
	return &peer
}

func (self *Peer) backgroundRefresh() {
	// Long-running method to keep peering info up to date
	for {
		select {
		case <-self.quitChan:
			return
		default:
			self.Refresh()
		}
		time.Sleep(1e9)
	}
}

func (self *Peer) Refresh() {
	// Ask remote for its peer list
	now := time.Now()
	binds := self.client.GetPeers()
	ok := binds != nil
	ok = ok && len(binds)%2 == 0
	if !ok {
		return
	}
	// Update local info
	for i := 0; i < len(binds); i += 2 {
		bind := binds[i]
		p, exists := self.member.peers[bind]
		if !exists {
			self.member.AddPeers(bind)
			continue
		}
		lastSeen, err := strconv.Atoi(binds[i+1])
		if err != nil {
			continue
		}
		p.lastSeen = now.Unix() - int64(lastSeen)
	}
	self.lastSeen = now.Unix()
}
