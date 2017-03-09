package ingress

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

type ResolutionFailure string

func (err ResolutionFailure) Error() string {
	return fmt.Sprintf("Resolving host failed: %s", string(err))
}

type IPRange struct {
	Start string
	End   string
}

type IPRanges struct {
	ranges []IPRange
}

func NewIPRanges(ranges ...IPRange) (*IPRanges, error) {
	r := &IPRanges{ranges: ranges}

	err := r.validate()
	if err != nil {
		return nil, err
	}

	return r, nil

}

func (i *IPRanges) validate() error {
	for _, ipRange := range i.ranges {
		startIP := net.ParseIP(ipRange.Start)
		endIP := net.ParseIP(ipRange.End)
		if startIP == nil {
			return errors.New(fmt.Sprintf("Invalid IP Address for Blacklist IP Range: %s", ipRange.Start))
		}
		if endIP == nil {
			return errors.New(fmt.Sprintf("Invalid IP Address for Blacklist IP Range: %s", ipRange.End))
		}
		if bytes.Compare(startIP, endIP) > 0 {
			return errors.New(fmt.Sprintf("Invalid Blacklist IP Range: Start %s has to be before End %s", ipRange.Start, ipRange.End))
		}
	}
	return nil
}

func (i *IPRanges) IpOutsideOfRanges(drainURL string) (bool, error) {
	testURL, _ := url.Parse(drainURL)

	if len(testURL.Host) == 0 {
		return false, errors.New(fmt.Sprintf("Incomplete URL %s. "+
			"This could be caused by an URL without slashes or protocol.", testURL))
	}

	host := strings.Split(testURL.Host, ":")[0]
	ipAddress := net.ParseIP(host)
	if ipAddress == nil {
		ipAddr, err := net.ResolveIPAddr("ip", host)
		if err != nil {
			return false, ResolutionFailure(err.Error())
		}
		ipAddress = net.ParseIP(ipAddr.String())
	}

	for _, ipRange := range i.ranges {
		if bytes.Compare(ipAddress, net.ParseIP(ipRange.Start)) >= 0 && bytes.Compare(ipAddress, net.ParseIP(ipRange.End)) <= 0 {
			return false, nil
		}
	}
	return true, nil
}
