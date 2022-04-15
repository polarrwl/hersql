package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Orlion/hersql/config"
	"github.com/Orlion/hersql/log"
	"github.com/Orlion/hersql/server"
)

var confFilename string

func init() {
	flag.StringVar(&confFilename, "conf", "conf.yml", "Please enter a configuration file name")
}

func main() {
	flag.Parse()
	conf, err := config.Parse(confFilename)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	server, err := server.NewServer(conf, log.GetLogger(conf))
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	server.Start()
}
