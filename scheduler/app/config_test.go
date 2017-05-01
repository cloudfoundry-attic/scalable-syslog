package app_test

import (
	"code.cloudfoundry.org/scalable-syslog/scheduler/app"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {
	Describe("Blacklisting IPs", func() {
		It("takes a string and converts to a IpRanges", func() {
			blacklist := "12.12.12.0-12.12.12.10,1.1.1.1-1.1.1.10"

			args := []string{
				"--adapter-cn", "default-string",
				"--adapter-addrs", "10.10.10.10",
				"--adapter-port", "default-string",
				"--api-ca", "default-string",
				"--api-cert", "default-string",
				"--api-cn", "default-string",
				"--api-key", "default-string",
				"--api-url", "default-string",
				"--ca", "default-string",
				"--cert", "default-string",
				"--key", "default-string",
				"--metric-ingress-addr", "default-string",
				"--metric-ingress-cn", "default-string",
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
				"--adapter-addrs", "10.10.10.10",
				"--adapter-port", "default-string",
				"--api-ca", "default-string",
				"--api-cert", "default-string",
				"--api-cn", "default-string",
				"--api-key", "default-string",
				"--api-url", "default-string",
				"--ca", "default-string",
				"--cert", "default-string",
				"--key", "default-string",
				"--metric-ingress-addr", "default-string",
				"--metric-ingress-cn", "default-string",
			}
			_, err := app.LoadConfig(args)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Describe("Adapter Addresses", func() {
		It("allows an empty value", func() {
			args := []string{
				"--adapter-addrs", "10.10.10.10,10.10.10.11",
				"--adapter-cn", "default-string",
				"--adapter-port", "8080",
				"--api-ca", "default-string",
				"--api-cert", "default-string",
				"--api-cn", "default-string",
				"--api-key", "default-string",
				"--api-url", "default-string",
				"--ca", "default-string",
				"--cert", "default-string",
				"--key", "default-string",
				"--metric-ingress-addr", "default-string",
				"--metric-ingress-cn", "default-string",
			}
			cfg, err := app.LoadConfig(args)
			Expect(err).ToNot(HaveOccurred())
			Expect(cfg.AdapterAddrs).To(Equal([]string{
				"10.10.10.10:8080",
				"10.10.10.11:8080",
			}))
		})
	})
})
