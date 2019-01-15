package franz

import (
	"time"
)

const (
	UP = iota
	DOWN
)

type Status int
type PeerList []*Peer

type Peer struct {
	Client *Client
	Status Status
}
type Member struct {
	Bind  string
	Peers []*Peer
}

func NewMember(bind string, peers []string) *Member {
	member := Member{
		bind,
		NewPeerList(peers),
	}
	go member.discover()
	return &member
}

func NewPeerList(binds []string) PeerList {
	peerList := make([]*Peer, len(binds))
	for pos, bind := range binds {
		peerList[pos] = NewPeer(bind)
	}
	return peerList
}

func NewPeer(bind string) *Peer {
	client := NewClient(bind)
	peer := Peer{client, UP}
	return &peer
}

func (self *Member) discover() {
	for {
		for _, peer := range self.Peers {
			ok := peer.Ping()
			if !ok {
				println("Peer is down")
			}
		}
		time.Sleep(1000000)
	}
}

func (self *Member) AddPeers(peers []string) {
	// TODO
}

func (self *Peer) Ping() bool {
	ok := self.Client.Ping()
	if ok {
		self.Status = UP
	} else {
		// TODO we may want to retry a bit later before deciding on Status
		self.Status = DOWN
	}
	return ok
}
