package franz

import (
	"fmt"
	"github.com/yawn/netstring"
	"golang.org/x/net/websocket"
	"log"
	"net/http"
	"os"
	"time"
)

const address string = "localhost:1234"

func launch_server() {
	http.Handle("/ws", websocket.Handler(Echo))
	println("Server started")
	if err := http.ListenAndServe(":1234", nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}

func launch_client() {
	fmt.Println("Starting Client")
	ws, err := websocket.Dial(fmt.Sprintf("ws://%s/ws", address), "", fmt.Sprintf("http://%s/", address))
	if err != nil {
		fmt.Printf("Dial failed: %s\n", err.Error())
		os.Exit(1)
	}
	go readClientMessages(ws)
	i := 0
	for {
		i++
		msg, err := netstring.Encode([]byte("foo"), []byte("bar"))
		if err != nil {
			log.Fatal(err)
		}
		err = websocket.Message.Send(ws, msg)
		if err != nil {
			fmt.Printf("Write failed: %s\n", err.Error())
			os.Exit(1e3)
		}
		fmt.Println(i)
		time.Sleep(1e9)
	}
}

func readClientMessages(ws *websocket.Conn) {
	for {
		var message []byte
		err := websocket.Message.Receive(ws, &message)
		if err != nil {
			fmt.Printf("Error::: %s\n", err.Error())
			return
		}
		fmt.Println(string(message))
	}
}