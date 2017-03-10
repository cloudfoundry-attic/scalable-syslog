package app_test

import (
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {
	Context("For blacklisting IPs", func() {
		It("takes a string and converts to a IpRanges", func() {
			blacklist := "12.12.12.0-12.12.12.10,1.1.1.1-1.1.1.10"

			args := []string{
				"--adapter-cn", "default-string",
				"--adapter-ips", "10.10.10.10",
				"--adapter-port", "default-string",
				"--api-ca", "default-string",
				"--api-cert", "default-string",
				"--api-cn", "default-string",
				"--api-key", "default-string",
				"--api-url", "default-string",
				"--ca", "default-string",
				"--cert", "default-string",
				"--key", "default-string",
				"--blacklist-ranges", blacklist,
			}
			config, err := app.LoadConfig(args)
			Expect(err).ToNot(HaveOccurred())

			Expect(config.Blacklist.Ranges).To(ContainElement(
				ingress.IPRange{Start: "12.12.12.0", End: "12.12.12.10"},
			))
			Expect(config.Blacklist.Ranges).To(ContainElement(
				ingress.IPRange{Start: "1.1.1.1", End: "1.1.1.10"},
			))
		})

		It("allows an empty value", func() {
			args := []string{
				"--adapter-cn", "default-string",
				"--adapter-ips", "10.10.10.10",
				"--adapter-port", "default-string",
				"--api-ca", "default-string",
				"--api-cert", "default-string",
				"--api-cn", "default-string",
				"--api-key", "default-string",
				"--api-url", "default-string",
				"--ca", "default-string",
				"--cert", "default-string",
				"--key", "default-string",
			}
			_, err := app.LoadConfig(args)
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
