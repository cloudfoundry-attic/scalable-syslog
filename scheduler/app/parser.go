package app

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

func ParseAddrs(ips, port string) ([]string, error) {
	var hostports []string

	if len(ips) == 0 {
		return nil, errors.New("no IP addresses provided")
	}

	hosts := strings.Split(ips, ",")

	for _, h := range hosts {
		if net.ParseIP(h) == nil {
			return nil, fmt.Errorf("invalid IP format: %s", h)
		}
		hp := fmt.Sprintf("%s:%s", h, port)
		hostports = append(hostports, hp)
	}

	return hostports, nil
}
