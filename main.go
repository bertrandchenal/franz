package main
import (
	// "golang.org/x/exp/mmap" 
    "golang.org/x/net/websocket"
    "fmt"
    "log"
    "net/http"
)

type Stream struct {
	Name string
}


func NewStream(name string) *Stream {
	// TODO
	// - create blob & index files if none found
	// - create metadata file (with list of buckets their start time,
	//   start offset, and some descriptions
	// - instanciate mmap	
	stream := &Stream{
		Name: name,
	}
	return stream
}

// func (self *Stream) Append(data []byte) error {
// }

// func (self *Stream) Read(offset int64) ([]byte, error) {
// }


// func (self *Stream) Info() ?? {
// }


func Echo(ws *websocket.Conn) {
    var err error

    for {
        var reply string
        if err = websocket.Message.Receive(ws, &reply); err != nil {
            fmt.Println("Can't receive")
            break
        }

        fmt.Println("Received back from client: " + reply)
        msg := "Received:  " + reply
        fmt.Println("Sending to client: " + msg)

        if err = websocket.Message.Send(ws, msg); err != nil {
            fmt.Println("Can't send")
            break
        }
    }
}

func main() {
    http.Handle("/ws", websocket.Handler(Echo))
    if err := http.ListenAndServe(":1234", nil); err != nil {
        log.Fatal("ListenAndServe:", err)
    }
}
