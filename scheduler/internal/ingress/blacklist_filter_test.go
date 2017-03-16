package ingress_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	v1 "github.com/cloudfoundry-incubator/scalable-syslog/api/v1"
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
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
			v1.Binding{AppId: "app-id-with-good-drain", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
		}
		bindingReader := SpyBindingReader{Bindings: input}

		filter := ingress.NewBlacklistFilter(blacklistIps, bindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(input))
	})

	It("removes bindings with an invalid host", func() {
		input := ingress.Bindings{
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://some.invalid.host"},
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
			v1.Binding{AppId: "app-id-with-bad-drain", Hostname: "we.dont.care", Drain: "syslog://invalid.host"},
		}
		expected := ingress.Bindings{
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
		}
		bindingReader := SpyBindingReader{Bindings: input}

		filter := ingress.NewBlacklistFilter(blacklistIps, bindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(expected))
	})

	It("removes bindings for a blacklisted IP", func() {
		input := ingress.Bindings{
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://14.15.16.18"},
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
		}
		expected := ingress.Bindings{
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
		}
		bindingReader := SpyBindingReader{Bindings: input}

		filter := ingress.NewBlacklistFilter(blacklistIps, bindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(expected))
	})

	It("removes bindings for incompleted schemes", func() {
		input := ingress.Bindings{
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "http://"},
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
		}
		expected := ingress.Bindings{
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
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
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://14.15.16.18"},
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://14.15.16.20"},
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.15"},
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
