package main

import (
    "golang.org/x/net/websocket"
	"fmt"
	"os"
	"time"
)


const address string = "localhost:1234"

func main() {
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
		err = websocket.Message.Send(ws, "Hiya")
		if err != nil {
			fmt.Printf("Send failed: %s\n", err.Error())
			os.Exit(1)
		}
		fmt.Println(i)
		time.Sleep(1000)
	}
}


func readClientMessages(ws *websocket.Conn) {
    for {
        var message string
        // err := websocket.JSON.Receive(ws, &message)
        err := websocket.Message.Receive(ws, &message)
        if err != nil {
            fmt.Printf("Error::: %s\n", err.Error())
            return
        }
        fmt.Println(message)
    }
}
