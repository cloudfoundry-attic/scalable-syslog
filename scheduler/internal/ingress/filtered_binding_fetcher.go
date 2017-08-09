package ingress

import (
	"fmt"
	"log"
	"net"

	loggregator "code.cloudfoundry.org/go-loggregator"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

type IPChecker interface {
	ParseHost(url string) (string, error)
	ResolveAddr(host string) (net.IP, error)
	CheckBlacklist(ip net.IP) error
}

// LogClient is used to emit logs about an applications syslog drain.
type LogClient interface {
	EmitLog(message string, opts ...loggregator.EmitLogOption)
}

type FilteredBindingFetcher struct {
	ipChecker IPChecker
	br        BindingReader
	logClient LogClient
}

func NewFilteredBindingFetcher(c IPChecker, b BindingReader, lc LogClient) *FilteredBindingFetcher {
	return &FilteredBindingFetcher{
		ipChecker: c,
		br:        b,
		logClient: lc,
	}
}

func (f *FilteredBindingFetcher) FetchBindings() ([]v1.Binding, int, error) {
	sourceBindings, err := f.br.FetchBindings()
	if err != nil {
		return nil, 0, err
	}
	newBindings := []v1.Binding{}

	for _, binding := range sourceBindings {
		host, err := f.ipChecker.ParseHost(binding.Drain)
		if err != nil {
			log.Println(err)
			f.emitErrorLog(binding.AppId, "Invalid syslog drain URL: parse failure")
			continue
		}

		ip, err := f.ipChecker.ResolveAddr(host)
		if err != nil {
			msg := fmt.Sprintf("Failed to resolve syslog drain host: %s", host)
			log.Println(msg, err)
			f.emitErrorLog(binding.AppId, msg)
			continue
		}

		err = f.ipChecker.CheckBlacklist(ip)
		if err != nil {
			msg := fmt.Sprintf("Syslog drain blacklisted: %s (%s)", host, ip)
			log.Println(msg, err)
			f.emitErrorLog(binding.AppId, msg)
			continue
		}

		newBindings = append(newBindings, binding)
	}

	removed := len(sourceBindings) - len(newBindings)
	return newBindings, removed, nil
}

func (f *FilteredBindingFetcher) emitErrorLog(appID, message string) {
	option := loggregator.WithAppInfo(
		appID,
		"LGR",
		"", // source instance is unavailable
	)
	f.logClient.EmitLog(message, option)
}
