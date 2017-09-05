package main
import (
	// "golang.org/x/exp/mmap" 
    "golang.org/x/net/websocket"
	"github.com/yawn/netstring"
    "fmt"
    "log"
    "net/http"
	// "path"
)

type Tube struct {
	Root string
	Name string
}


func NewTube(name string) *Tube {
	// TODO
	// - create directory, blob & index files if none found
	// - create metadata file (with list of buckets their start time,
	//   start offset, and some descriptions
	// - instanciate mmap	
	tube := &Tube{
		Name: name,
	}
	return tube
}

// func (self *Tube) Append(data []byte) error {
// }

// func (self *Tube) Read(offset int64) ([]byte, error) {
// }


// func (self *Tube) Info() ?? {
// }


func Echo(ws *websocket.Conn) {
    var err error

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
	fmt.Println("Server started")
    if err := http.ListenAndServe(":1234", nil); err != nil {
        log.Fatal("ListenAndServe:", err)
    }
}
