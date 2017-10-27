package ingress

import (
	"fmt"
	"math/rand"
	"net"
)

// IPBalancer provides IPs resolved from a DNS address in random order
type IPBalancer struct {
	addr   string
	lookup func(string) ([]net.IP, error)
}

// IPBalancerOption is a type that will manipulate a config
type IPBalancerOption func(*IPBalancer)

// WithLookup sets the behavior of looking up IPs
func WithLookup(lookup func(string) ([]net.IP, error)) func(*IPBalancer) {
	return func(b *IPBalancer) {
		b.lookup = lookup
	}
}

// NewIPBalancer returns an IPBalancer
func NewIPBalancer(addr string, opts ...IPBalancerOption) *IPBalancer {
	balancer := &IPBalancer{
		addr:   addr,
		lookup: net.LookupIP,
	}

	for _, o := range opts {
		o(balancer)
	}

	return balancer
}

// NextHostPort returns hostport resolved from the balancer's addr.
// It returns error for an invalid addr or if lookup failed or
// doesn't resolve to anything.
func (b *IPBalancer) NextHostPort() (string, error) {
	host, port, err := net.SplitHostPort(b.addr)
	if err != nil {
		return "", err
	}

	ips, err := b.lookup(host)
	if err != nil {
		return "", err
	}

	if len(ips) == 0 {
		return "", fmt.Errorf("lookup failed with addr %s", b.addr)
	}

	return net.JoinHostPort(ips[rand.Int()%len(ips)].String(), port), nil

}
