package main

import (
	"github.com/bertrandchenal/franz"
	"flag"
)

func main() {
	var address = flag.String("address", ":9090", "Address to listen to")
	var root_path = flag.String("root", ".", "Root data directory")
	flag.Parse()

	server := franz.NewServer(*root_path, *address)
	server.Run()
}
