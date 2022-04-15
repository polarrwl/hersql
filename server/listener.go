package server

import (
	"net"
)

type Listener struct {
	net.Listener
	h *Handler
}

func newListener(protocol, address string, handler *Handler) (*Listener, error) {
	l, err := net.Listen(protocol, address)
	if err != nil {
		return nil, err
	}
	return &Listener{l, handler}, nil
}

func (l *Listener) Accept() (net.Conn, error) {
	return l.Listener.Accept()
}
