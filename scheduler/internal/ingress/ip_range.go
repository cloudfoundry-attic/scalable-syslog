package ingress

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

type IPRange struct {
	Start string
	End   string
}

type IPRanges struct {
	Ranges []IPRange
}

func NewIPRanges(ranges ...IPRange) (*IPRanges, error) {
	r := &IPRanges{Ranges: ranges}

	err := r.validate()
	if err != nil {
		return nil, err
	}

	return r, nil

}

func (i *IPRanges) validate() error {
	for _, ipRange := range i.Ranges {
		startIP := net.ParseIP(ipRange.Start)
		endIP := net.ParseIP(ipRange.End)
		if startIP == nil {
			return fmt.Errorf("invalid IP Address for Blacklist IP Range: %s", ipRange.Start)
		}
		if endIP == nil {
			return fmt.Errorf("invalid IP Address for Blacklist IP Range: %s", ipRange.End)
		}
		if bytes.Compare(startIP, endIP) > 0 {
			return fmt.Errorf("invalid Blacklist IP Range: Start %s has to be before End %s", ipRange.Start, ipRange.End)
		}
	}
	return nil
}

func (i *IPRanges) IpOutsideOfRanges(drainURL string) error {
	testURL, err := url.Parse(drainURL)
	if err != nil {
		return err
	}

	if len(testURL.Host) == 0 {
		return errors.New("invalid URL, detected no host")
	}

	host := strings.Split(testURL.Host, ":")[0]

	ipAddress := net.ParseIP(host)
	if ipAddress == nil {
		ipAddr, err := net.ResolveIPAddr("ip", host)
		if err != nil {
			return fmt.Errorf("unable to resolve DNS entry: %s", testURL.Host)
		}
		ipAddress = net.ParseIP(ipAddr.String())
	}

	for _, ipRange := range i.Ranges {
		if bytes.Compare(ipAddress, net.ParseIP(ipRange.Start)) >= 0 && bytes.Compare(ipAddress, net.ParseIP(ipRange.End)) <= 0 {
			return fmt.Errorf("syslog drain blacklisted: %s", testURL.Host)
		}
	}
	return nil
}
