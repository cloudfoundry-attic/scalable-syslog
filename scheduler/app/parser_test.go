package app_test

import (
	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ParseAddrs", func() {
	It("parses an IP address", func() {
		addrs, _ := app.ParseAddrs("1.2.3.4", "1234")

		Expect(len(addrs)).To(Equal(1))
		Expect(addrs[0]).To(Equal("1.2.3.4:1234"))
	})

	It("parses IP addresses separated by commas", func() {
		addrs, _ := app.ParseAddrs("1.2.3.4,9.8.7.6", "1234")

		Expect(len(addrs)).To(Equal(2))
		Expect(addrs[0]).To(Equal("1.2.3.4:1234"))
		Expect(addrs[1]).To(Equal("9.8.7.6:1234"))
	})

	It("returns an error when the IP addresses are empty", func() {
		_, err := app.ParseAddrs("", "")

		Expect(err).To(HaveOccurred())
	})

	It("returns an error when the IP address is malformatted", func() {
		_, err := app.ParseAddrs("bad IP address", "1234")

		Expect(err).To(HaveOccurred())
	})
})
