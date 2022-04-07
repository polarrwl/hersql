package main

import "github.com/Orlion/lakeman/internal/server"

func main() {
	server := server.NewServer()
	server.Start()
}
