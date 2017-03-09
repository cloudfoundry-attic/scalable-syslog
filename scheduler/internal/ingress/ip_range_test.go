package ingress_test

import (
	"fmt"

	"github.com/cloudfoundry-incubator/scalable-syslog/scheduler/internal/ingress"
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
			Expect(err).To(MatchError("Invalid IP Address for Blacklist IP Range: 127.0.2.2.1"))
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
			Expect(err).To(MatchError("Invalid Blacklist IP Range: Start 10.10.10.10 has to be before End 10.8.10.12"))
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

			for _, ipTest := range ipTests {
				outOfRange, err := ranges.IpOutsideOfRanges(ipTest.url)
				Expect(err).ToNot(HaveOccurred())
				Expect(outOfRange).To(Equal(ipTest.output), fmt.Sprintf("Wrong output for url: %s", ipTest.url))
			}
		})

		It("returns error on malformatted URL", func() {
			ranges, _ := ingress.NewIPRanges(
				ingress.IPRange{Start: "127.0.2.2", End: "127.0.2.4"},
			)

			for _, testUrl := range malformattedURLs {
				_, err := ranges.IpOutsideOfRanges(testUrl)
				Expect(err).To(HaveOccurred())
			}
		})

		It("always returns true when ip ranges is empty", func() {
			ranges, _ := ingress.NewIPRanges()

			outSideOfRange, err := ranges.IpOutsideOfRanges("https://127.0.0.1")
			Expect(err).NotTo(HaveOccurred())
			Expect(outSideOfRange).To(BeTrue())
		})

		It("resolves ip addresses", func() {
			ranges, _ := ingress.NewIPRanges(
				ingress.IPRange{Start: "127.0.0.0", End: "127.0.0.4"},
			)

			outSideOfRange, err := ranges.IpOutsideOfRanges("syslog://vcap.me:3000?app=great")
			Expect(err).NotTo(HaveOccurred())
			Expect(outSideOfRange).To(BeFalse())

			outSideOfRange, err = ranges.IpOutsideOfRanges("syslog://localhost:3000?app=great")
			Expect(err).NotTo(HaveOccurred())
			Expect(outSideOfRange).To(BeFalse())

			outSideOfRange, err = ranges.IpOutsideOfRanges("syslog://doesNotExist.local:3000?app=great")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Resolving host failed: "))
		})
	})

})

var ipTests = []struct {
	url    string
	output bool
}{
	{"http://127.0.0.1", true},
	{"http://127.0.1.1", true},
	{"http://127.0.3.5", true},
	{"http://127.0.2.2", false},
	{"http://127.0.2.3", false},
	{"http://127.0.2.4", false},
	{"https://127.0.1.1", true},
	{"https://127.0.2.3", false},
	{"syslog://127.0.1.1", true},
	{"syslog://127.0.2.3", false},
	{"syslog://127.0.1.1:3000", true},
	{"syslog://127.0.2.3:3000", false},
	{"syslog://127.0.1.1:3000/test", true},
	{"syslog://127.0.2.3:3000/test", false},
	{"syslog://127.0.1.1:3000?app=great", true},
	{"syslog://127.0.2.3:3000?app=great", false},
	{"syslog://127.0.2.3:3000?app=great", false},
}

var malformattedURLs = []string{
	"127.0.0.1:300/new",
	"syslog:127.0.0.1:300/new",
	"<nil>",
}
