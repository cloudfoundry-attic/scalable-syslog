package ingress_test

import (
	"errors"
	"net"
	"net/url"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	v2 "code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
)

var _ = Describe("FilteredBindingFetcher", func() {
	It("returns valid bindings", func() {
		input := []v1.Binding{
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
			v1.Binding{AppId: "app-id-with-multiple-drains", Hostname: "we.dont.care", Drain: "syslog://10.10.10.12"},
			v1.Binding{AppId: "app-id-with-good-drain", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
		}
		bindingReader := &SpyBindingReader{bindings: input}

		filter := ingress.NewFilteredBindingFetcher(&spyIPChecker{}, bindingReader, &spyLogClient{})
		actual, removed, err := filter.FetchBindings()

		Expect(err).ToNot(HaveOccurred())
		Expect(actual).To(Equal(input))
		Expect(removed).To(Equal(0))
	})

	It("returns an error if the binding reader cannot fetch bindings", func() {
		bindingReader := &SpyBindingReader{nil, errors.New("Woops")}

		filter := ingress.NewFilteredBindingFetcher(&spyIPChecker{}, bindingReader, &spyLogClient{})
		actual, _, err := filter.FetchBindings()

		Expect(err).To(HaveOccurred())
		Expect(actual).To(BeNil())
	})

	Context("when syslog drain has invalid host", func() {
		var (
			filter    *ingress.FilteredBindingFetcher
			logClient *spyLogClient
		)

		BeforeEach(func() {
			input := []v1.Binding{
				v1.Binding{AppId: "app-id", Hostname: "we.dont.care", Drain: "syslog://some.invalid.host"},
			}

			logClient = &spyLogClient{}

			filter = ingress.NewFilteredBindingFetcher(
				&spyIPChecker{parseHostError: errors.New("parse error")},
				&SpyBindingReader{bindings: input},
				logClient,
			)
		})

		It("removes the binding", func() {
			actual, removed, err := filter.FetchBindings()

			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(Equal([]v1.Binding{}))
			Expect(removed).To(Equal(1))
		})

		It("emitts a LGR error", func() {
			_, _, _ = filter.FetchBindings()

			Expect(logClient.calledWith).To(Equal("Invalid syslog drain URL: parse failure"))
			Expect(logClient.appID).To(Equal("app-id"))
			Expect(logClient.sourceType).To(Equal("LGR"))
		})
	})

	Context("when syslog drain has invalid scheme", func() {
		var (
			filter *ingress.FilteredBindingFetcher
			input  []v1.Binding
		)

		BeforeEach(func() {
			input = []v1.Binding{
				v1.Binding{AppId: "app-id", Hostname: "we.dont.care", Drain: "syslog://10.10.10.10"},
				v1.Binding{AppId: "app-id", Hostname: "we.dont.care", Drain: "syslog-tls://10.10.10.10"},
				v1.Binding{AppId: "app-id", Hostname: "we.dont.care", Drain: "https://10.10.10.10"},
				v1.Binding{AppId: "app-id", Hostname: "we.dont.care", Drain: "bad-scheme://10.10.10.10"},
				v1.Binding{AppId: "app-id", Hostname: "we.dont.care", Drain: "blah://10.10.10.10"},
			}

			filter = ingress.NewFilteredBindingFetcher(
				&spyIPChecker{},
				&SpyBindingReader{bindings: input},
				&spyLogClient{},
			)
		})

		It("ignores the bindings", func() {
			actual, removed, err := filter.FetchBindings()

			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(Equal(input[:3]))
			Expect(removed).To(Equal(2))
		})
	})

	Context("when the drain host fails to resolve", func() {
		var (
			filter    *ingress.FilteredBindingFetcher
			logClient *spyLogClient
		)

		BeforeEach(func() {
			input := []v1.Binding{
				v1.Binding{AppId: "app-id", Hostname: "we.dont.care", Drain: "syslog://some.invalid.host"},
			}

			logClient = &spyLogClient{}

			filter = ingress.NewFilteredBindingFetcher(
				&spyIPChecker{
					resolveAddrError: errors.New("resolve error"),
					parsedHost:       "some.invalid.host",
				},
				&SpyBindingReader{bindings: input},
				logClient,
			)
		})

		It("removes bindings that failed to resolve", func() {
			actual, removed, err := filter.FetchBindings()

			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(Equal([]v1.Binding{}))
			Expect(removed).To(Equal(1))
		})

		It("emitts a LGR error", func() {
			_, _, _ = filter.FetchBindings()

			Expect(logClient.calledWith).To(Equal("Failed to resolve syslog drain host: some.invalid.host"))
			Expect(logClient.appID).To(Equal("app-id"))
			Expect(logClient.sourceType).To(Equal("LGR"))
		})
	})

	Context("when the syslog drain has been blacklisted", func() {
		var (
			filter    *ingress.FilteredBindingFetcher
			logClient *spyLogClient
		)

		BeforeEach(func() {
			input := []v1.Binding{
				v1.Binding{AppId: "app-id", Hostname: "we.dont.care", Drain: "syslog://some.invalid.host"},
			}

			logClient = &spyLogClient{}

			filter = ingress.NewFilteredBindingFetcher(
				&spyIPChecker{
					checkBlacklistError: errors.New("blacklist error"),
					parsedHost:          "some.invalid.host",
					resolvedIP:          net.ParseIP("127.0.0.1"),
				},
				&SpyBindingReader{bindings: input},
				logClient,
			)
		})

		It("removes the binding", func() {
			actual, removed, err := filter.FetchBindings()

			Expect(err).ToNot(HaveOccurred())
			Expect(actual).To(Equal([]v1.Binding{}))
			Expect(removed).To(Equal(1))
		})

		It("emitts a LGR error", func() {
			_, _, _ = filter.FetchBindings()

			Expect(logClient.calledWith).To(Equal("Syslog drain blacklisted: some.invalid.host (127.0.0.1)"))
			Expect(logClient.appID).To(Equal("app-id"))
			Expect(logClient.sourceType).To(Equal("LGR"))
		})
	})
})

type spyIPChecker struct {
	checkBlacklistError error
	resolveAddrError    error
	resolvedIP          net.IP
	parseHostError      error
	parsedScheme        string
	parsedHost          string
}

func (s *spyIPChecker) CheckBlacklist(net.IP) error {
	return s.checkBlacklistError
}

func (s *spyIPChecker) ParseHost(URL string) (string, string, error) {
	u, err := url.Parse(URL)
	if err != nil {
		panic(err)
	}

	return u.Scheme, s.parsedHost, s.parseHostError
}

func (s *spyIPChecker) ResolveAddr(host string) (net.IP, error) {
	return s.resolvedIP, s.resolveAddrError
}

type spyLogClient struct {
	calledWith string
	appID      string
	sourceType string
}

func (s *spyLogClient) EmitLog(message string, opts ...loggregator.EmitLogOption) {
	s.calledWith = message
	env := &v2.Envelope{
		Tags: make(map[string]string),
	}
	for _, o := range opts {
		o(env)
	}
	s.appID = env.SourceId
	s.sourceType = env.GetTags()["source_type"]
}

type SpyBindingReader struct {
	bindings []v1.Binding
	err      error
}

func (s *SpyBindingReader) FetchBindings() ([]v1.Binding, error) {
	return s.bindings, s.err
}
