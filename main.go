package main
import "golang.org/x/exp/mmap" 
import "fmt"

type Stream struct {
	Name string
}


func NewStream(name string) error {
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

func (self *Stream) Append(data []byte) error {
}

func (self *Stream) Read(offset int64) ([]byte, error) {
}


func (self *Stream) Info() ?? {
}

func main() {
	// reader, err := mmap.Open("data")
	// println(err)
	// defer reader.Close()
	// p := make([]byte, 3)
	// size, err := reader.ReadAt(p, 3)
	// fmt.Printf("%q", p[:size])
}
