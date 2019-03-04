package franz

import (
	"testing"
	"time"
)

func filterString(list []string, to_filter string) []string {
	res := make([]string, 0, len(list)-1)
	for _, item := range list {
		if item == to_filter {
			continue
		}
		res = append(res, item)
	}
	return res
}

func setup(binds []string) []*Server {
	servers := make([]*Server, len(binds))
	for pos, bind := range binds {
		others := filterString(binds, bind)
		root := TEST_DIR
		server := NewServer(root, bind)
		go server.Run()
		server.Join(others)
		servers[pos] = server
	}
	return servers
}

func TestPing(t *testing.T) {
	binds := []string{"localhost:9090", "localhost:9091", "localhost:9092"}
	servers := setup(binds)
	delay := int64(2)
	// Sleep a bit to let servers discover each others
	time.Sleep(time.Duration(delay) * time.Second)

	// Check if all servers are seeing each others
	up_count := 0
	now := time.Now().Unix()
	for _, server := range servers {
		for _, peer := range server.member.peers {
			if peer.lastSeen >= now-delay {
				up_count += 1
			}
		}
	}
	if up_count != 6 {
		t.Errorf("Expected 6, got: %v", up_count)
	}

	// Stop one server
	servers[0].Shutdown()
	time.Sleep(time.Duration(delay) * time.Second)
	up_count = 0
	now = time.Now().Unix()
	for _, server := range servers[1:] {
		for _, peer := range server.member.peers {
			if peer.lastSeen > now-delay {
				up_count += 1
			}
		}
	}
	if up_count != 2 {
		t.Errorf("Expected 2, got: %v", up_count)
	}
}
