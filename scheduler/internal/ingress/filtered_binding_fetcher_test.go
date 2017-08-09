package ingress_test

import (
	"errors"
	"net"

	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

var _ = Describe("BlacklistFilter", func() {
	It("returns valid bindings", func() {
		input := []v1.Binding{
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
			v1.Binding{AppId: "app-id-with-good-drain", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
		}
		bindingReader := &SpyBindingReader{bindings: input}

		filter := ingress.NewFilteredBindingFetcher(&spyIPChecker{}, bindingReader)
		actual, removed, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(input))
		Expect(removed).To(Equal(0))
	})

	It("returns an error if the binding reader cannot fetch bindings", func() {
		bindingReader := &SpyBindingReader{nil, errors.New("Woops")}

		filter := ingress.NewFilteredBindingFetcher(&spyIPChecker{}, bindingReader)
		actual, _, err := filter.FetchBindings()

		Expect(err).To(HaveOccurred())
		Expect(actual).To(BeNil())
	})

	Context("when syslog drain has invalid host", func() {
		It("removes the binding", func() {
			input := []v1.Binding{
				v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://some.invalid.host"},
			}
			bindingReader := &SpyBindingReader{bindings: input}

			expected := []v1.Binding{}
			filter := ingress.NewFilteredBindingFetcher(&spyIPChecker{parseHostError: errors.New("parse error")}, bindingReader)
			actual, removed, err := filter.FetchBindings()

			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(Equal(expected))
			Expect(removed).To(Equal(1))
		})
	})

	Context("when the drain host fails to resolve", func() {
		It("removes bindings that failed to resolve", func() {
			input := []v1.Binding{
				v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "http://"},
			}
			expected := []v1.Binding{}
			bindingReader := &SpyBindingReader{bindings: input}

			filter := ingress.NewFilteredBindingFetcher(&spyIPChecker{resolveAddrError: errors.New("resolve error")}, bindingReader)
			actual, removed, err := filter.FetchBindings()

			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(Equal(expected))
			Expect(removed).To(Equal(1))
		})
	})

	Context("when the syslog drain has been blacklisted", func() {
		It("removes the binding", func() {
			input := []v1.Binding{
				v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://14.15.16.18"},
			}
			bindingReader := &SpyBindingReader{bindings: input}

			expected := []v1.Binding{}
			filter := ingress.NewFilteredBindingFetcher(&spyIPChecker{checkBlacklistError: errors.New("blacklist error")}, bindingReader)
			actual, removed, err := filter.FetchBindings()

			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(Equal(expected))
			Expect(removed).To(Equal(1))
		})
	})
})

type spyIPChecker struct {
	checkBlacklistError error
	resolveAddrError    error
	parseHostError      error
}

func (s *spyIPChecker) CheckBlacklist(net.IP) error {
	return s.checkBlacklistError
}

func (s *spyIPChecker) ParseHost(string) (string, error) {
	return "", s.parseHostError
}

func (s *spyIPChecker) ResolveAddr(host string) (net.IP, error) {
	return nil, s.resolveAddrError
}
