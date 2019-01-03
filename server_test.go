package franz

import (
	"fmt"
	"bitbucket.org/bertrandchenal/netstring"
	"golang.org/x/net/websocket"
	"log"
	// "net/http"
	"os"
	"testing"
	"time"
)

const address string = "localhost:8080"

func TestServer(t *testing.T) {
	root := TEST_DIR
	bind := ":8080"
	server := NewServer(&root, &bind)
	go server.Run()

	ws, err := websocket.Dial(fmt.Sprintf("ws://%s/ws", address), "", fmt.Sprintf("http://%s/", address))
	if err != nil {
		fmt.Printf("Dial failed: %s\n", err.Error())
		os.Exit(1)
	}

	answer := sendMessage(ws)
	if answer != "OK" {
		t.Error("Unexpected value:", answer)
	}

	time.Sleep(1e9)
	readMessage(ws)
}

func sendMessage(ws *websocket.Conn) string {
	msg, err := netstring.Encode(
		[]byte("publish"),     //Action
		[]byte("server-test"), //Tube
		[]byte("bar"),         // Message
	)
	if err != nil {
		log.Fatal(err)
	}
	err = websocket.Message.Send(ws, msg)
	if err != nil {
		fmt.Printf("Write failed: %s\n", err.Error())
		os.Exit(1e3)
	}
	var message []byte
	err = websocket.Message.Receive(ws, &message)
	if err != nil {
		panic(err)
	}
	return string(message)
}

func readMessage(ws *websocket.Conn) {
	// TODO
}

// func launchServer() {
// }
