package ingress_test

import (
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("IPRanges", func() {
	Describe("validates", func() {
		It("IP address range", func() {
			_, err := ingress.NewIPRanges(
				ingress.IPRange{Start: "127.0.2.2", End: "127.0.2.4"},
			)
			Expect(err).ToNot(HaveOccurred())
		})

		It("start address", func() {
			_, err := ingress.NewIPRanges(
				ingress.IPRange{Start: "127.0.2.2.1", End: "127.0.2.4"},
			)
			Expect(err).To(MatchError("invalid IP Address for Blacklist IP Range: 127.0.2.2.1"))
		})

		It("end address", func() {
			_, err := ingress.NewIPRanges(
				ingress.IPRange{Start: "127.0.2.2", End: "127.0.2.4.3"},
			)
			Expect(err).To(HaveOccurred())
		})

		It("given IP addresses", func() {
			_, err := ingress.NewIPRanges(
				ingress.IPRange{Start: "127.0.2.2", End: "127.0.2.4"},
				ingress.IPRange{Start: "127.0.2.2", End: "127.0.2.4.5"},
			)
			Expect(err).To(HaveOccurred())
		})

		It("start IP is before end IP", func() {
			_, err := ingress.NewIPRanges(
				ingress.IPRange{Start: "10.10.10.10", End: "10.8.10.12"},
			)
			Expect(err).To(MatchError("invalid Blacklist IP Range: Start 10.10.10.10 has to be before End 10.8.10.12"))
		})

		It("accepts start and end as the same", func() {
			_, err := ingress.NewIPRanges(
				ingress.IPRange{Start: "127.0.2.2", End: "127.0.2.2"},
			)
			Expect(err).ToNot(HaveOccurred())
		})

	})

	Describe("IpOutsideOfRanges", func() {
		It("parses the IP address properly", func() {
			ranges, err := ingress.NewIPRanges(
				ingress.IPRange{Start: "127.0.1.2", End: "127.0.3.4"},
			)
			Expect(err).ToNot(HaveOccurred())

			for _, url := range validIPs {
				err := ranges.IpOutsideOfRanges(url)
				Expect(err).ToNot(HaveOccurred())
			}

			for _, url := range invalidIPs {
				err := ranges.IpOutsideOfRanges(url)
				Expect(err).To(HaveOccurred())
			}
		})

		It("returns error on malformatted URL", func() {
			ranges, _ := ingress.NewIPRanges(
				ingress.IPRange{Start: "127.0.2.2", End: "127.0.2.4"},
			)

			for _, testUrl := range malformattedURLs {
				err := ranges.IpOutsideOfRanges(testUrl)
				Expect(err).To(HaveOccurred(), "url: "+testUrl)
			}
		})

		It("allows all urls for empty blacklist range", func() {
			ranges, _ := ingress.NewIPRanges()

			err := ranges.IpOutsideOfRanges("https://127.0.0.1")
			Expect(err).NotTo(HaveOccurred())
		})

		It("resolves ip addresses", func() {
			ranges, _ := ingress.NewIPRanges(
				ingress.IPRange{Start: "127.0.0.0", End: "127.0.0.4"},
			)

			err := ranges.IpOutsideOfRanges("syslog://vcap.me:3000?app=great")
			Expect(err).To(HaveOccurred())

			err = ranges.IpOutsideOfRanges("syslog://localhost:3000?app=great")
			Expect(err).To(HaveOccurred())

			err = ranges.IpOutsideOfRanges("syslog://example:3000?app=great")
			Expect(err).To(HaveOccurred())
		})
	})

})

var validIPs = []string{
	"http://127.0.0.1",
	"http://127.0.1.1",
	"http://127.0.3.5",
	"https://127.0.1.1",
	"syslog://127.0.1.1",
	"syslog://127.0.1.1:3000",
	"syslog://127.0.1.1:3000/test",
	"syslog://127.0.1.1:3000?app=great",
}

var invalidIPs = []string{
	"http://127.0.2.2",
	"http://127.0.2.3",
	"http://127.0.2.4",
	"https://127.0.2.3",
	"syslog://127.0.2.3",
	"syslog://127.0.2.3:3000",
	"syslog://127.0.2.3:3000/test",
	"syslog://127.0.2.3:3000?app=great",
	"://127.0.2.3:3000?app=great",
}

var malformattedURLs = []string{
	"127.0.0.1:300/new",
	"syslog:127.0.0.1:300/new",
	"<nil>",
}
