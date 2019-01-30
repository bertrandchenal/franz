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
	go member.discover()
	return &member
}

func (self *Member) AddPeers(binds ...string) {
	for _, bind := range binds {
		self.peers[bind] = NewPeer(bind, self)
	}
}

func NewPeer(bind string, member *Member) *Peer {
	client := NewClient("ws://" + bind + "/ws")
	peer := Peer{bind: bind, client: client, status: UP, member: member}
	return &peer
}

func (self *Member) discover() {
	for {
		for _, peer := range self.peers {
			ok := peer.Refresh()
			// TODO update own list of peers
			if !ok {
				continue
			}
		}
		time.Sleep(1e9)
	}
}

func (self *Peer) Refresh() bool {
	// FIXME may fail if remote is not yet up -> use strict timeout!
	binds := self.client.GetPeers()
	ok := binds != nil
	ok = ok && len(binds)%2 == 0
	now := time.Now()
	if !ok {
		self.status = DOWN
		return false
	}
	for i := 0; i < len(binds); i++ {
		bind := binds[i]
		p, exists := self.member.peers[bind]
		if !exists {
			continue // TODO call AddPeers
		}
		lastSeen, err := strconv.Atoi(binds[i+1])
		if err != nil {
			continue
		}
		p.lastSeen = now.Unix() - int64(lastSeen)
	}
	self.status = UP
	self.lastSeen = now.Unix()

	return true
}
