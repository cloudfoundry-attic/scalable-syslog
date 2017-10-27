package ingress_test

import (
	"errors"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"code.cloudfoundry.org/scalable-syslog/adapter/internal/ingress"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/test_util"
	"code.cloudfoundry.org/scalable-syslog/internal/api"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connector", func() {
	It("returns a client", func() {
		cleanup, addr := startGRPCServer()
		defer cleanup()
		b := &stubBalancer{nextHostPort: addr}
		tlsConf, err := api.NewMutualTLSConfig(
			test_util.Cert("adapter-rlp.crt"),
			test_util.Cert("adapter-rlp.key"),
			test_util.Cert("loggregator-ca.crt"),
			"fake-log-provider",
		)
		Expect(err).ToNot(HaveOccurred())
		c := ingress.NewConnector(b, 1*time.Second, tlsConf)

		_, client, err := c.Connect()

		Expect(err).ToNot(HaveOccurred())
		Expect(client).ToNot(BeNil())
	})

	It("returns an error when the balancer fails", func() {
		b := &stubBalancer{nextHostPortErr: errors.New("no host port")}
		tlsConf, err := api.NewMutualTLSConfig(
			test_util.Cert("adapter-rlp.crt"),
			test_util.Cert("adapter-rlp.key"),
			test_util.Cert("loggregator-ca.crt"),
			"fake-log-provider",
		)
		Expect(err).ToNot(HaveOccurred())
		c := ingress.NewConnector(b, 1*time.Millisecond, tlsConf)

		_, _, err = c.Connect()

		Expect(err).To(HaveOccurred())
	})

	It("returns an error when the dialing fails", func() {
		b := &stubBalancer{nextHostPort: "localhost:1985"}
		tlsConf, err := api.NewMutualTLSConfig(
			test_util.Cert("adapter-rlp.crt"),
			test_util.Cert("adapter-rlp.key"),
			test_util.Cert("loggregator-ca.crt"),
			"fake-log-provider",
		)
		Expect(err).ToNot(HaveOccurred())
		c := ingress.NewConnector(b, 1*time.Nanosecond, tlsConf)

		_, _, err = c.Connect()

		Expect(err).To(HaveOccurred())
	})
})

type stubBalancer struct {
	nextHostPort    string
	nextHostPortErr error
}

func (s *stubBalancer) NextHostPort() (string, error) {
	return s.nextHostPort, s.nextHostPortErr
}

func startGRPCServer() (func(), string) {
	lis, err := net.Listen("tcp", "localhost:0")
	Expect(err).ToNot(HaveOccurred())

	tlsConfig, err := api.NewMutualTLSConfig(
		test_util.Cert("fake-log-provider.crt"),
		test_util.Cert("fake-log-provider.key"),
		test_util.Cert("loggregator-ca.crt"),
		"fake-log-provider",
	)
	s := grpc.NewServer(grpc.Creds(
		credentials.NewTLS(tlsConfig)),
	)
	go s.Serve(lis)

	return s.Stop, lis.Addr().String()
}
