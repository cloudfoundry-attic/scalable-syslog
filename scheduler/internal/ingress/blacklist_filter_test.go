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
		input := ingress.AppBindings{
			"app-id-with-multiple-drains": ingress.Binding{
				Drains: []string{
					"syslog://10.10.10.10",
					"syslog://10.10.10.12",
				},
				Hostname: "we.dont.care",
			},
			"app-id-with-good-drain": ingress.Binding{
				Drains: []string{
					"syslog://10.10.10.10",
				},
				Hostname: "we.dont.care",
			},
		}
		bindingReader := SpyBindingReader{Bindings: input}

		filter := ingress.NewBlacklistFilter(blacklistIps, bindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(input))
	})

	It("removes bindings with an invalid host", func() {
		input := ingress.AppBindings{
			"app-id-with-multiple-drains": ingress.Binding{
				Drains: []string{
					"syslog://some.invalid.host",
					"syslog://10.10.10.12",
				},
				Hostname: "we.dont.care",
			},
			"app-id-with-bad-drain": ingress.Binding{
				Drains: []string{
					"syslog://invalid.host",
				},
				Hostname: "we.dont.care",
			},
		}
		expected := ingress.AppBindings{
			"app-id-with-multiple-drains": ingress.Binding{
				Drains: []string{
					"syslog://10.10.10.12",
				},
				Hostname: "we.dont.care",
			},
		}
		bindingReader := SpyBindingReader{Bindings: input}

		filter := ingress.NewBlacklistFilter(blacklistIps, bindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(expected))
	})

	It("removes bindings for a blacklisted IP", func() {
		input := ingress.AppBindings{
			"app-id-with-multiple-drains": ingress.Binding{
				Drains: []string{
					"syslog://14.15.16.18",
					"syslog://10.10.10.12",
				},
				Hostname: "we.dont.care",
			},
		}
		expected := ingress.AppBindings{
			"app-id-with-multiple-drains": ingress.Binding{
				Drains: []string{
					"syslog://10.10.10.12",
				},
				Hostname: "we.dont.care",
			},
		}
		bindingReader := SpyBindingReader{Bindings: input}

		filter := ingress.NewBlacklistFilter(blacklistIps, bindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(expected))
	})

	It("removes bindings for incompleted schemes", func() {
		input := ingress.AppBindings{
			"app-id-with-multiple-drains": ingress.Binding{
				Drains: []string{
					"http://",
					"syslog://10.10.10.12",
				},
				Hostname: "we.dont.care",
			},
		}
		expected := ingress.AppBindings{
			"app-id-with-multiple-drains": ingress.Binding{
				Drains: []string{
					"syslog://10.10.10.12",
				},
				Hostname: "we.dont.care",
			},
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
})

type SpyBindingReader struct {
	Bindings ingress.AppBindings
	Err      error
}

func (s SpyBindingReader) FetchBindings() (ingress.AppBindings, error) {
	return s.Bindings, s.Err
}
