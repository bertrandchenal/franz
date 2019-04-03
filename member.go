package franz

import (
	"hash/crc32"
	"sort"
	"strconv"
	"time"
)

const (
	UP = iota
	DOWN
)

type Status int
type ShardList []*Shard
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
	ring  ShardList
}

type Shard struct {
	sum  uint32
	peer *Peer
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
	// TODO couple ring refresh with peer status (wait for peer list
	// to stabilize before updating ring)
	self.ring = self.HashRing()
}

func (self *Member) RemovePeer(bind string) {
	self.peers[bind].quitChan <- true
}

func (self *Member) HashRing() ShardList {
	// Compute several hashes per peer
	nbShards := 5
	shards := make(ShardList, len(self.peers)*nbShards)
	peerCount := 0
	padding := []byte("pad")
	for bindString, peer := range self.peers {
		bind := []byte(bindString)
		for i := 0; i < nbShards; i++ {
			h := crc32.ChecksumIEEE(bind)
			shards[peerCount] = &Shard{h, peer}
			peerCount += 1
			bind = append(bind, padding...)
		}
	}
	sort.Sort(shards)
	return shards
}

func (self *Member) FindPeer(tubeName []byte) *Peer {
	// TODO implement lock around the ring (when updating it & when
	// findpeer is called.

	// Find the shard closest to tubeName checksum and return the
	// corresponding peer.
	h := crc32.ChecksumIEEE(tubeName)
	pos := sort.Search(len(self.ring), func(i int) bool {
		shard := self.ring[i]
		return shard.sum >= h
	})
	if pos == len(self.ring) {
		pos = 0
	}

	return self.ring[pos].peer
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
		delta, err := strconv.Atoi(binds[i+1])
		if err != nil {
			continue
		}
		lastSeen := now.Unix() - int64(delta)
		if lastSeen > p.lastSeen {
			p.lastSeen = lastSeen
		}
	}
	self.lastSeen = now.Unix()
}

// Makes Shards sortable
func (shards ShardList) Len() int {
	return len(shards)
}
func (shards ShardList) Swap(i, j int) {
	shards[i], shards[j] = shards[j], shards[i]
}
func (shards ShardList) Less(i, j int) bool {
	return shards[i].sum < shards[j].sum
}
