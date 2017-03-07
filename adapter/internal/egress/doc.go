package egress

import "net"

//go:generate hel

type Dialer interface {
	Dial(network, address string) (conn net.Conn, err error)
}

type Conn interface {
	net.Conn
}
