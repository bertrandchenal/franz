package franz

import (
	"testing"
)

const address string = "localhost:8080"

func TestServer(t *testing.T) {
	root := TEST_DIR
	bind := "localhost:8080"
	server := NewServer(root, bind)
	go server.Run()

	client := NewClient("ws://" + bind + "/ws")
	msg := []byte("hello")
	answer := client.Publish("test", msg)
	if string(answer) != "OK" {
		t.Error("Unexpected value:", answer)
	}
}

