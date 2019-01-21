package franz

import (
	"log"
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
	Peers []string
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
	client := NewClient("ws://" + bind + "/ws")
	peer := Peer{client, UP, nil}
	return &peer
}

func (self *Member) discover() {
	for {
		for _, peer := range self.Peers {
			ok := peer.GetPeers()
			// TODO update own list of peers
			if !ok {
				println("Peer is down")
			}
		}
		time.Sleep(1e9)
	}
}

func (self *Member) AddPeers(peers []string) {
	// TODO
}

func (self *Peer) GetPeers() bool {
	peers := self.Client.Peers() // FIXME without a timeout this may never return
	ok := peers != nil
	if ok {
		self.Status = UP
		self.Peers = peers
		log.Printf("%v", peers)
	} else {
		// TODO we may want to retry a bit later before deciding on Status
		self.Status = DOWN
	}
	return ok
}
