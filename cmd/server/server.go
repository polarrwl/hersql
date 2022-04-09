package main

import (
	"fmt"
	"os"
	"time"

	"github.com/Orlion/lakeman/internal/log"
	"github.com/Orlion/lakeman/internal/server"
	"github.com/dolthub/go-mysql-server/auth"
	dserver "github.com/dolthub/go-mysql-server/server"
)

func main() {
	server, err := server.NewServer(dserver.Config{
		Protocol:               "tcp",
		Address:                "127.0.0.1:3306",
		Version:                "5.7.1",
		ConnReadTimeout:        5 * time.Second,
		ConnWriteTimeout:       5 * time.Second,
		MaxConnections:         5,
		TLSConfig:              nil,
		RequireSecureTransport: false,
		Auth:                   auth.NewNativeSingle("root", "123456", auth.AllPermissions),
	}, log.GetLogger())
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	server.Start()
}
