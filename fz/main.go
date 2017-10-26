package main

import (
	"bitbucket.org/bertrandchenal/franz"
	"flag"
)


func main() {
	var address = flag.String("address", ":8080", "Address to listen to")
	var root = flag.String("root", ".", "Root data directory")

	server := Server(root_path, address)
	server.Run()
}
