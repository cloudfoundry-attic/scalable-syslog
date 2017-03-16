package ingress_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BlacklistFilter", func() {
	var blacklistIps *ingress.IPRanges

	BeforeEach(func() {
		var err error
		blacklistIps, err = ingress.NewIPRanges(
			ingress.IPRange{Start: "14.15.16.17", End: "14.15.16.20"},
		)
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns valid bindings", func() {
		input := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
			ingress.Binding{AppID: "app-id-with-good-drain", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
		}
		bindingReader := SpyBindingReader{Bindings: input}

		filter := ingress.NewBlacklistFilter(blacklistIps, bindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(input))
	})

	It("removes bindings with an invalid host", func() {
		input := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://some.invalid.host"},
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
			ingress.Binding{AppID: "app-id-with-bad-drain", Hostname: "we.dont.care", Drain: "syslog://invalid.host"},
		}
		expected := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
		}
		bindingReader := SpyBindingReader{Bindings: input}

		filter := ingress.NewBlacklistFilter(blacklistIps, bindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(expected))
	})

	It("removes bindings for a blacklisted IP", func() {
		input := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://14.15.16.18"},
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
		}
		expected := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
		}
		bindingReader := SpyBindingReader{Bindings: input}

		filter := ingress.NewBlacklistFilter(blacklistIps, bindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(expected))
	})

	It("removes bindings for incompleted schemes", func() {
		input := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "http://"},
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
		}
		expected := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
		}
		bindingReader := SpyBindingReader{Bindings: input}

		filter := ingress.NewBlacklistFilter(blacklistIps, bindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(expected))
	})

	It("returns an error if the binding reader cannot fetch bindings", func() {
		bindingReader := SpyBindingReader{nil, errors.New("Woops")}

		filter := ingress.NewBlacklistFilter(blacklistIps, bindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).To(HaveOccurred())
		Expect(actual).To(BeNil())
	})

	It("returns the drain count for non-blacklisted bindings", func() {
		input := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://14.15.16.18"},
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://14.15.16.20"},
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.15"},
		}
		spyBindingReader := SpyBindingReader{
			Bindings: input,
		}
		filter := ingress.NewBlacklistFilter(blacklistIps, spyBindingReader)

		Expect(filter.Count()).To(Equal(0))
		_, err := filter.FetchBindings()
		Expect(err).ToNot(HaveOccurred())
		Expect(filter.Count()).To(Equal(2))
	})
})

type SpyBindingReader struct {
	Bindings ingress.Bindings
	Err      error
}

func (s SpyBindingReader) FetchBindings() (ingress.Bindings, error) {
	return s.Bindings, s.Err
}
