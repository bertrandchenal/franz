package franz

import (
	"testing"
)

func filterString(list []string,  to_filter string) []string {
	res := make([]string, len(list) - 1)
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
		go server.Join(others)
		go server.Run()
		servers[pos] = server
	}
	return servers
}

func TestPing(t *testing.T) {
	binds := []string{"localhost:8080", "localhost:8081", "localhost:8082"}
	servers := setup(binds)
	println(len(servers))
}
