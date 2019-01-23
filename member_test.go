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
		go server.Join(others)
		servers[pos] = server
	}
	return servers
}

func TestPing(t *testing.T) {
	binds := []string{"localhost:9090", "localhost:9091", "localhost:9092"}
	servers := setup(binds)
	time.Sleep(2e9)

	// Check if all servers are seeing each others
	up_count := 0
	for _, server := range servers {
		for _, peer := range server.member.Peers {
			if peer.Status == UP {
				up_count += 1
			}
		}
	}
	if up_count != 6 {
		t.Errorf("Expected 6, got: %v", up_count)
	}

	// Stop one server
	servers[0].Shutdown()
	time.Sleep(3e9)
	up_count = 0
	for _, server := range servers[1:] {
		for _, peer := range server.member.Peers {
			if peer.Status == UP {
				up_count += 1
			}
		}
	}
	if up_count != 2 {
		t.Errorf("Expected 2, got: %v", up_count)
	}

}
