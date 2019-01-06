package main

import (
	"bitbucket.org/bertrandchenal/franz"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

func main() {
	var server = flag.String("server", "ws://localhost:9090/ws", "Server address")
	var publish = flag.String("p", "", "Publish in given tube")
	var subscribe = flag.String("s", "", "Subscribe in tube")
	flag.Parse()

	client := franz.NewClient(*server)
	if *publish != "" {
		var buffer bytes.Buffer

		chunk := make([]byte, 1024)
		var n int
		var err error
		for err != io.EOF {
			n, err = os.Stdin.Read(chunk)
			if n > 0 {
				buffer.Write(chunk[0:n])
			}
		}
		client.Publish(*publish, buffer.Bytes())
		log.Print("Done")

	} else if *subscribe != "" {
		client.Subscribe(*subscribe)

	} else {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
}
