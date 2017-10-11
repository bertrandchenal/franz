package main

import (
	// "golang.org/x/exp/mmap" 
    "golang.org/x/net/websocket"
	"github.com/yawn/netstring"
    // "fmt"
    "log"
    "net/http"
	// "path"
)



func Echo(ws *websocket.Conn) {
    var err error
	addr := ws.RemoteAddr().String()
	println(addr)

    for {
        var reply []byte
        if err = websocket.Message.Receive(ws, &reply); err != nil {
            log.Println("[RECV]", err)
            break
        }

		var items [][]byte
		if items, err = netstring.Decode(reply); err != nil {
            log.Println("[DECO]", err)
            break
		}
		for pos := range items {
			log.Println(string(items[pos]))
		}

        if err = websocket.Message.Send(ws, []byte("OK")); err != nil {
            log.Println("[SEND]", err)
            break
        }
    }
}

func main() {
    http.Handle("/ws", websocket.Handler(Echo))
	println("Server started")
    if err := http.ListenAndServe(":1234", nil); err != nil {
        log.Fatal("ListenAndServe:", err)
    }
}
