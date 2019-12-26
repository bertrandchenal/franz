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
	// Sleep a bit to let servers discover each others
	time.Sleep(time.Duration(2) * time.Second)
	return servers
}

func tearDown(servers []*Server) {
	for _, server := range servers {
		server.Shutdown()
	}
}

func TestPing(t *testing.T) {
	delay := int64(2)
	binds := []string{"localhost:9090", "localhost:9091", "localhost:9092"}
	servers := setup(binds)

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
	expected := len(binds) * len(binds)
	if up_count != expected {
		t.Errorf("Expected %v, got: %v", expected, up_count)
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
	expected = (len(binds) - 1) * (len(binds) - 1)
	if up_count != expected {
		t.Errorf("Expected %v, got: %v", expected, up_count)
	}

	tearDown(servers[1:])
}

func TestSharding(t *testing.T) {
	binds := []string{"localhost:9090", "localhost:9091", "localhost:9092"}
	servers := setup(binds)
	firstServer := servers[0]
	firstRing := firstServer.member.ring
	// Check that hash rings are in sync
	for server_pos, server := range servers[1:] {
		for pos, item := range server.member.ring {
			if item.sum != firstRing[pos].sum {
				t.Errorf(
					"Rings mismatch between first server and server %v at position %v",
					server_pos, pos)
			}
		}
	}

	servers[2].Shutdown()
	time.Sleep(time.Duration(2) * time.Second)
	for pos, item := range servers[1].member.ring {
		if item.sum != firstRing[pos].sum {
			t.Errorf("Rings mismatch between servers at position %v", pos)
		}
	}
	tearDown(servers[:2])
}
