package franz

import (
	// "golang.org/x/exp/mmap"
	"github.com/yawn/netstring"
	"golang.org/x/net/websocket"
	// "fmt"
	"log"
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
