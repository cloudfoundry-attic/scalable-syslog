package ingress_test

import (
	"errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
)

var _ = Describe("VersionFilter", func() {
	It("filters out bindings with drain URL with drain-version != 2.0", func() {
		input := ingress.AppBindings{
			"app-id-with-multiple-drains": ingress.Binding{
				Drains: []string{
					"syslog://example.com:1234/?drain-version=2.0",
					"syslog://example.net:4321/",
				},
				Hostname: "we.dont.care",
			},
			"app-id-with-good-drain": ingress.Binding{
				Drains: []string{
					"syslog://example.com:1234/?drain-version=2.0",
				},
				Hostname: "we.dont.care",
			},
			"app-id-with-bad-drain": ingress.Binding{
				Drains: []string{
					"syslog://example.net:4321/",
				},
				Hostname: "we.dont.care",
			},
		}
		expected := ingress.AppBindings{
			"app-id-with-multiple-drains": ingress.Binding{
				Drains: []string{
					"syslog://example.com:1234/?drain-version=2.0",
				},
				Hostname: "we.dont.care",
			},
			"app-id-with-good-drain": ingress.Binding{
				Drains: []string{
					"syslog://example.com:1234/?drain-version=2.0",
				},
				Hostname: "we.dont.care",
			},
		}
		mockBindingReader := newMockBindingReader()
		mockBindingReader.FetchBindingsOutput.AppBindings <- input
		mockBindingReader.FetchBindingsOutput.Err <- nil
		filter := ingress.NewVersionFilter(mockBindingReader)

		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(expected))
	})

	It("ignores malformed drain URLs", func() {
		input := ingress.AppBindings{
			"app-id-with-malformed-drains": ingress.Binding{
				Drains: []string{
					"://some-bad-url/?drain-version=2.0",
					"syslog://example.com:1234/?drain-version=2.0",
					"syslog://example.net:4321/",
				},
				Hostname: "we.dont.care",
			},
			"app-id-with-single-malformed-drain": ingress.Binding{
				Drains: []string{
					"://another-bad-url/?drain-version=2.0",
				},
				Hostname: "we.dont.care",
			},
		}
		expected := ingress.AppBindings{
			"app-id-with-malformed-drains": ingress.Binding{
				Drains: []string{
					"syslog://example.com:1234/?drain-version=2.0",
				},
				Hostname: "we.dont.care",
			},
		}
		mockBindingReader := newMockBindingReader()
		mockBindingReader.FetchBindingsOutput.AppBindings <- input
		mockBindingReader.FetchBindingsOutput.Err <- nil
		filter := ingress.NewVersionFilter(mockBindingReader)

		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(expected))
	})

	It("returns an error when wrapped BindingReader returns an error", func() {
		mockBindingReader := newMockBindingReader()
		mockBindingReader.FetchBindingsOutput.AppBindings <- nil
		mockBindingReader.FetchBindingsOutput.Err <- errors.New("some-error")
		filter := ingress.NewVersionFilter(mockBindingReader)

		_, err := filter.FetchBindings()

		Expect(err).To(HaveOccurred())
	})

	It("returns the drain count for drain-version=2.0", func() {
		input := ingress.AppBindings{
			"app-id-with-multiple-drains": ingress.Binding{
				Drains: []string{
					"syslog://example.com:1234/?drain-version=2.0",
					"syslog://example.net:4321/",
				},
				Hostname: "we.dont.care",
			},
			"app-id-with-good-drain": ingress.Binding{
				Drains: []string{
					"syslog://example.com:1234/?drain-version=2.0",
				},
				Hostname: "we.dont.care",
			},
			"app-id-with-bad-drain": ingress.Binding{
				Drains: []string{
					"syslog://example.net:4321/",
				},
				Hostname: "we.dont.care",
			},
		}
		mockBindingReader := newMockBindingReader()
		mockBindingReader.FetchBindingsOutput.AppBindings <- input
		mockBindingReader.FetchBindingsOutput.Err <- nil
		filter := ingress.NewVersionFilter(mockBindingReader)

		Expect(filter.Count()).To(Equal(0))

		_, err := filter.FetchBindings()
		Expect(err).ToNot(HaveOccurred())
		Expect(filter.Count()).To(Equal(2))
	})
})
