package server

import (
	"github.com/Orlion/hersql/config"
	"github.com/dolthub/vitess/go/mysql"
	"go.uber.org/zap"
)

type Server struct {
	Listener *mysql.Listener
	h        *Handler
	logger   *zap.SugaredLogger
}

func NewServer(conf *config.Conf, logger *zap.SugaredLogger) (*Server, error) {
	handler := newHandler(conf.Server.ConnReadTimeout, logger)

	l, err := NewListener(conf.Server.Protocol, conf.Server.Address, handler)
	if err != nil {
		return nil, err
	}

	listenerCfg := mysql.ListenerConfig{
		Listener:           l,
		AuthServer:         nil,
		Handler:            handler,
		ConnReadTimeout:    conf.Server.ConnReadTimeout,
		ConnWriteTimeout:   conf.Server.ConnWriteTimeout,
		MaxConns:           conf.Server.MaxConnections,
		ConnReadBufferSize: mysql.DefaultConnBufferSize,
	}
	vtListnr, err := mysql.NewListenerWithConfig(listenerCfg)
	if err != nil {
		return nil, err
	}

	if conf.Server.Version != "" {
		vtListnr.ServerVersion = conf.Server.Version
	}
	vtListnr.TLSConfig = nil
	vtListnr.RequireSecureTransport = false

	return &Server{Listener: vtListnr, h: handler, logger: logger}, nil
}

func (s *Server) Start() error {
	s.logger.Infow("server 启动...")
	s.Listener.Accept()
	return nil
}

func (s *Server) Close() error {
	s.Listener.Close()
	return nil
}
