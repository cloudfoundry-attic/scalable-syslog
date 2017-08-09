package ingress

import (
	"log"
	"net"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

type IPChecker interface {
	ParseHost(url string) (string, error)
	ResolveAddr(host string) (net.IP, error)
	CheckBlacklist(ip net.IP) error
}

type FilteredBindingFetcher struct {
	ipChecker IPChecker
	br        BindingReader
}

func NewFilteredBindingFetcher(c IPChecker, b BindingReader) *FilteredBindingFetcher {
	return &FilteredBindingFetcher{
		ipChecker: c,
		br:        b,
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
			continue
		}

		ip, err := f.ipChecker.ResolveAddr(host)
		if err != nil {
			log.Println(err)
			continue
		}

		err = f.ipChecker.CheckBlacklist(ip)
		if err != nil {
			log.Println(err)
			continue
		}

		newBindings = append(newBindings, binding)
	}

	removed := len(sourceBindings) - len(newBindings)
	return newBindings, removed, nil
}
