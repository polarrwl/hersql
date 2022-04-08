package server

import (
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/vitess/go/mysql"
)

type Server struct {
	Listener *mysql.Listener
	h        *Handler
}

func NewServer(cfg server.Config) (*Server, error) {
	if cfg.ConnReadTimeout < 0 {
		cfg.ConnReadTimeout = 0
	}

	if cfg.ConnWriteTimeout < 0 {
		cfg.ConnWriteTimeout = 0
	}

	if cfg.MaxConnections < 1 {
		cfg.MaxConnections = 0
	}

	handler := newHandler(cfg.ConnReadTimeout)

	a := cfg.Auth.Mysql()
	l, err := NewListener(cfg.Protocol, cfg.Address, handler)
	if err != nil {
		return nil, err
	}

	listenerCfg := mysql.ListenerConfig{
		Listener:           l,
		AuthServer:         a,
		Handler:            handler,
		ConnReadTimeout:    cfg.ConnReadTimeout,
		ConnWriteTimeout:   cfg.ConnWriteTimeout,
		MaxConns:           cfg.MaxConnections,
		ConnReadBufferSize: mysql.DefaultConnBufferSize,
	}
	vtListnr, err := mysql.NewListenerWithConfig(listenerCfg)
	if err != nil {
		return nil, err
	}

	if cfg.Version != "" {
		vtListnr.ServerVersion = cfg.Version
	}
	vtListnr.TLSConfig = cfg.TLSConfig
	vtListnr.RequireSecureTransport = cfg.RequireSecureTransport

	return &Server{Listener: vtListnr, h: handler}, nil
}

func (s *Server) Start() error {
	s.Listener.Accept()
	return nil
}

func (s *Server) Close() error {
	s.Listener.Close()
	return nil
}
