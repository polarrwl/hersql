package config

import "time"

type Server struct {
	Protocol         string
	Address          string
	Version          string
	ConnReadTimeout  time.Duration
	ConnWriteTimeout time.Duration
	MaxConnections   uint64
}

func Read() {

}
