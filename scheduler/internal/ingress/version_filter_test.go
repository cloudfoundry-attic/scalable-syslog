package ingress_test

import (
	"errors"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("VersionFilter", func() {

	It("returns an error if the binding reader cannot fetch bindings", func() {
		mockBindingReader := newMockBindingReader()
		mockBindingReader.FetchBindingsOutput.Bindings <- nil
		mockBindingReader.FetchBindingsOutput.Err <- errors.New("Woop")

		filter := ingress.NewVersionFilter(mockBindingReader)
		actual, err := filter.FetchBindings()

		Expect(err).To(HaveOccurred())
		Expect(actual).To(BeNil())
	})

	It("filters out bindings with drain URL with drain-version != 2.0", func() {
		input := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://example.com:1234/?drain-version=2.0"},
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://example.net:4321"},
			ingress.Binding{AppID: "app-id-with-good-drain", Hostname: "we.dont.care", Drain: "syslog://example.com:1234/?drain-version=2.0"},
			ingress.Binding{AppID: "app-id-with-bad-drain", Hostname: "we.dont.care", Drain: "syslog://example.com:1234"},
		}
		expected := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://example.com:1234/?drain-version=2.0"},
			ingress.Binding{AppID: "app-id-with-good-drain", Hostname: "we.dont.care", Drain: "syslog://example.com:1234/?drain-version=2.0"},
		}
		mockBindingReader := newMockBindingReader()
		mockBindingReader.FetchBindingsOutput.Bindings <- input
		mockBindingReader.FetchBindingsOutput.Err <- nil
		filter := ingress.NewVersionFilter(mockBindingReader)

		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(expected))
	})

	It("ignores malformed drain URLs", func() {
		input := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "://some-bad-url/?drain-version=2.0"},
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://example.net:4321"},
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://example.com:1234/?drain-version=2.0"},
			ingress.Binding{AppID: "app-id-with-malformed-drains", Hostname: "we.dont.care", Drain: "://another-bad-url/?drain-version=2.0"},
		}
		expected := ingress.Bindings{
			ingress.Binding{AppID: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://example.com:1234/?drain-version=2.0"},
		}
		mockBindingReader := newMockBindingReader()
		mockBindingReader.FetchBindingsOutput.Bindings <- input
		mockBindingReader.FetchBindingsOutput.Err <- nil
		filter := ingress.NewVersionFilter(mockBindingReader)

		actual, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(expected))
	})

	It("returns an error when wrapped BindingReader returns an error", func() {
		mockBindingReader := newMockBindingReader()
		mockBindingReader.FetchBindingsOutput.Bindings <- nil
		mockBindingReader.FetchBindingsOutput.Err <- errors.New("some-error")
		filter := ingress.NewVersionFilter(mockBindingReader)

		_, err := filter.FetchBindings()

		Expect(err).To(HaveOccurred())
	})

})
