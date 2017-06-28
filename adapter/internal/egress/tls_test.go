package egress_test

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"time"

	"code.cloudfoundry.org/scalable-syslog/adapter/internal/egress"
	"code.cloudfoundry.org/scalable-syslog/adapter/internal/test_util"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	v1 "code.cloudfoundry.org/scalable-syslog/internal/api/v1"
	"code.cloudfoundry.org/scalable-syslog/internal/metricemitter"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TLSWriter", func() {
	It("speaks TLS", func() {
		certFile := test_util.Cert("adapter-rlp.crt")
		keyFile := test_util.Cert("adapter-rlp.key")
		tlsCert, err := tls.LoadX509KeyPair(certFile, keyFile)
		Expect(err).ToNot(HaveOccurred())
		tlsConfig := &tls.Config{
			Certificates:       []tls.Certificate{tlsCert},
			InsecureSkipVerify: true,
		}

		listener, err := tls.Listen("tcp", ":0", tlsConfig)
		Expect(err).ToNot(HaveOccurred())

		binding := &v1.Binding{
			AppId:    "test-app-id",
			Hostname: "test-hostname",
			Drain:    fmt.Sprintf("syslog-tls://%s", listener.Addr()),
		}
		egressCounter := new(metricemitter.CounterMetric)
		writer, err := egress.NewTLSWriter(binding, time.Second, time.Second, true, egressCounter)
		Expect(err).ToNot(HaveOccurred())
		defer writer.Close()

		conn, err := listener.Accept()
		Expect(err).ToNot(HaveOccurred())
		buf := bufio.NewReader(conn)

		// Note: for some odd reason you have to do a read off of the TLS
		// connection before the dial will succeed. We should probably
		// investigate.
		empty := make([]byte, 0)
		conn.Read(empty)

		env := buildLogEnvelope("APP", "2", "just a test", loggregator_v2.Log_OUT)
		f := func() error {
			return writer.Write(env)
		}
		Eventually(f).Should(Succeed())

		actual, err := buf.ReadString('\n')
		Expect(err).ToNot(HaveOccurred())

		expected := fmt.Sprintf("87 <14>1 1970-01-01T00:00:00.012345678Z test-hostname test-app-id [APP/2] - - just a test\n")
		Expect(actual).To(Equal(expected))

		By("emit an egress metric for each message")
		Expect(egressCounter.GetDelta()).To(Equal(uint64(1)))
	})
})
