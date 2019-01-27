package franz

import (
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
	peers    []string
	lastSeen int64
}
type Member struct {
	bind  string
	peers map[string]*Peer
}

func NewMember(bind string, peers []string) *Member {
	member := Member{
		bind,
		NewPeerMap(peers),
	}
	go member.discover()
	return &member
}

func NewPeerMap(binds []string) map[string]*Peer {
	peerMap := make(map[string]*Peer)
	for _, bind := range binds {
		peerMap[bind] = NewPeer(bind)
	}
	return peerMap
}

func NewPeer(bind string) *Peer {
	client := NewClient("ws://" + bind + "/ws")
	peer := Peer{bind: bind, client: client, status: UP}
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

func (self *Member) AddPeers(peers []string) {
	// TODO
}

func (self *Peer) Refresh() bool {
	// FIXME may fail if remote is not yet up -> use strict timeout!
	peers := self.client.GetPeers()
	ok := peers != nil
	now := time.Now()
	if ok {
		if len(peers) > 1 {
			// TODO update siblings lastSeen
			println(peers[0], peers[1])
		}
		self.status = UP
		self.lastSeen = now.Unix()
		self.peers = peers
	} else {
		// TODO we may want to retry a bit later before deciding on Status
		self.status = DOWN
	}
	return ok
}
