package franz

import (
	"testing"
	// "time"
)

const address string = "localhost:9090"

func TestServerPublish(t *testing.T) {
	root := TEST_DIR
	bind := "localhost:9090"
	server := NewServer(root, bind)
	go server.Run()

	client := NewClient("ws://" + bind + "/ws")
	msg := []byte("hello")
	answer := client.Publish("test", msg)
	if string(answer) != "OK" {
		t.Error("Unexpected value:", answer)
	}
}
