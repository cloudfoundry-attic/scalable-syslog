package api_test

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"code.cloudfoundry.org/scalable-syslog/internal/api"
)

var _ = Describe("TLS", func() {

	Context("NewMutalTLSConfig", func() {
		var (
			clientCertFilename  string
			clientKeyFilename   string
			caCertFilename      string
			nonSignedCAFilename string
		)

		BeforeEach(func() {
			clientCertFilename = writeFile(clientCert)
			clientKeyFilename = writeFile(clientKey)
			caCertFilename = writeFile(caCert)
			nonSignedCAFilename = writeFile(nonSignedCACert)
		})

		AfterEach(func() {
			err := os.Remove(clientCertFilename)
			Expect(err).ToNot(HaveOccurred())
			err = os.Remove(clientKeyFilename)
			Expect(err).ToNot(HaveOccurred())
			err = os.Remove(caCertFilename)
			Expect(err).ToNot(HaveOccurred())
		})

		It("builds a config struct", func() {
			conf, err := api.NewMutualTLSConfig(
				clientCertFilename,
				clientKeyFilename,
				caCertFilename,
				"test-server-name",
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(conf.Certificates).To(HaveLen(1))
			Expect(conf.InsecureSkipVerify).To(BeFalse())
			Expect(conf.ClientAuth).To(Equal(tls.RequireAndVerifyClientCert))
			Expect(conf.MinVersion).To(Equal(uint16(tls.VersionTLS12)))
			Expect(conf.CipherSuites).To(ConsistOf(
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			))

			Expect(string(conf.RootCAs.Subjects()[0])).To(ContainSubstring("scalableSyslogCA"))
			Expect(string(conf.ClientCAs.Subjects()[0])).To(ContainSubstring("scalableSyslogCA"))

			Expect(conf.ServerName).To(Equal("test-server-name"))
		})

		It("allows you to not specify a CA cert", func() {
			conf, err := api.NewMutualTLSConfig(
				clientCertFilename,
				clientKeyFilename,
				"",
				"",
			)
			Expect(err).ToNot(HaveOccurred())

			Expect(conf.RootCAs).To(BeNil())
			Expect(conf.ClientCAs).To(BeNil())
		})

		It("returns an error when given invalid cert/key paths", func() {
			_, err := api.NewMutualTLSConfig("", "", caCertFilename, "")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("failed to load keypair: open : no such file or directory"))
		})

		It("returns an error when given invalid ca cert path", func() {
			_, err := api.NewMutualTLSConfig(clientCertFilename, clientKeyFilename, "/file/that/does/not/exist", "")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("failed to read ca cert file: open /file/that/does/not/exist: no such file or directory"))
		})

		It("returns an error when given invalid ca cert file", func() {
			empty := writeFile("")
			defer func() {
				err := os.Remove(empty)
				Expect(err).ToNot(HaveOccurred())
			}()
			_, err := api.NewMutualTLSConfig(clientCertFilename, clientKeyFilename, empty, "")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("unable to load ca cert file"))
		})

		It("returns an error when the certificate is not signed by the CA", func() {
			_, err := api.NewMutualTLSConfig(clientCertFilename, clientKeyFilename, nonSignedCAFilename, "")
			Expect(err).To(HaveOccurred())
			_, ok := err.(api.CASignatureError)
			Expect(ok).To(BeTrue())
		})
	})

	Context("NewTLSConfig", func() {
		It("returns basic TLS config", func() {
			tlsConf := api.NewTLSConfig()
			Expect(tlsConf.InsecureSkipVerify).To(BeFalse())
			Expect(tlsConf.ClientAuth).To(Equal(tls.NoClientCert))
			Expect(tlsConf.MinVersion).To(Equal(uint16(tls.VersionTLS12)))
			Expect(tlsConf.CipherSuites).To(ContainElement(tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256))
			Expect(tlsConf.CipherSuites).To(ContainElement(tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384))
		})
	})
})

func writeFile(data string) string {
	f, err := ioutil.TempFile("", "")
	Expect(err).ToNot(HaveOccurred())
	_, err = fmt.Fprintf(f, data)
	Expect(err).ToNot(HaveOccurred())
	return f.Name()
}

var (
	clientCert = `
-----BEGIN CERTIFICATE-----
MIIEJjCCAg6gAwIBAgIRAO4nVQTDQmkwwNFGN9cO3fMwDQYJKoZIhvcNAQELBQAw
GzEZMBcGA1UEAxMQc2NhbGFibGVTeXNsb2dDQTAeFw0xOTAyMDYxNzQ4MTNaFw00
NDAyMDYxNzQ4MDlaMA8xDTALBgNVBAMTBGNhcGkwggEiMA0GCSqGSIb3DQEBAQUA
A4IBDwAwggEKAoIBAQDS729IL1FNYIalzlx7goE81Xvcc4ElmOrpHJ55lGJAuq74
cQqaib/YXnIdt9vYPoZjp/z6r7mffxpcSZ03ZV+rrZc65IOUSvsfyqbe8HSXs/sl
fbcywDmPx4ZYTeK40YcJMQQ8LgT6M9CwW28aES7GNi7qpiPlk9Lc5JeuMcFlcZE/
jM7KDAtGQZ/M9o1cV0dseXZZVEfa5bvWPTVJx7QnsN6wBeM8s2kuxboaygQyWJtf
O09hSUAMVE9w1HyDCzOiy/uFP8CjRdGe3S+T0c90itUEcC8XEAYpizeVflnUyhlh
E9p89P1YIXf63KtsdF2vTIodBjw9rsdB6KoKWEAhAgMBAAGjcTBvMA4GA1UdDwEB
/wQEAwIDuDAdBgNVHSUEFjAUBggrBgEFBQcDAQYIKwYBBQUHAwIwHQYDVR0OBBYE
FFQiz8y3kTXBohgd5xogY9WnIVtNMB8GA1UdIwQYMBaAFOe90SrAan4qhMrUve8z
hLjwpJoaMA0GCSqGSIb3DQEBCwUAA4ICAQC4Soad1LNaW7aGpAApPAaZWP0eIQ7H
U2Oi+afzElffk5PzhWql5Ft5qJH+q4Lu4C7H8cfB/g4qkhEN1l+KhELzAilRGwlI
CPVRDgmt2V1tNaQEXL8ewSCtAOe5aeAsZI8Q5eUCrkY8jA1DTerZzBwv5YnRXz+v
NPrg3FrjExFXO7jVqcJrbrjfsLWF3ZVrlX2SHi8RlhL1l2309OuWN1gPXnIsUAbv
zGUX4Jh8tVo5qm6UmqWUDzgjG2oVJc2JImqWVDVZA2DO0j3oH8jR2wlwVGg6XJzD
n8FhewnwXLznXg6agTdyoNBY+00fgOUcEXRknp6K9AHXjQYkB9TIIJx7NTVuB5m/
bpC3ynhSWnI/+82v7S+amFfsF49uNUOQ4yB8cKFi6CHborIiUdkah9AbcVgw4JS+
U1Q14klUsvUBBwxsWXAKobwFr5LUAHmixqZWbprm+jElrN7mO3sPZDTh4e+Queaa
mJB/lMeji865YPA+Wgvyl348KprurNemdrDYVSdJDlNrh4OYrVcyaQELVCH8IJOR
6L/i7GCTq4mfg7XfhRsPOYVgYPp6eGnGXwWqRfJnYdGk0Eoy2D5p2U6bSRXhiZ+i
WTv76zTpv5pSKALloHAzcMoWHgW+H6d5Oc+gXXAKKwZIY5JD22OHGklQcs9I5/eU
MGk8x6vBuUPJCg==
-----END CERTIFICATE-----`

	clientKey = `
-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEA0u9vSC9RTWCGpc5ce4KBPNV73HOBJZjq6RyeeZRiQLqu+HEK
mom/2F5yHbfb2D6GY6f8+q+5n38aXEmdN2Vfq62XOuSDlEr7H8qm3vB0l7P7JX23
MsA5j8eGWE3iuNGHCTEEPC4E+jPQsFtvGhEuxjYu6qYj5ZPS3OSXrjHBZXGRP4zO
ygwLRkGfzPaNXFdHbHl2WVRH2uW71j01Sce0J7DesAXjPLNpLsW6GsoEMlibXztP
YUlADFRPcNR8gwszosv7hT/Ao0XRnt0vk9HPdIrVBHAvFxAGKYs3lX5Z1MoZYRPa
fPT9WCF3+tyrbHRdr0yKHQY8Pa7HQeiqClhAIQIDAQABAoIBAQCgV7YF70IkSY3R
GE8g7BGFxtqCt6Kii912GnU1AmC1x7Md1AA1fSTCOvkQMiUB+nV558bcYRv/bEUD
hknmRD83Z5uf/vkujtjaT9gNfEeE0iHFXA0YnRqkew1arsn7p/q4N/R6MplCAIVQ
qhCIhitCLQn5ARYke/w8QeAzYEsc6pgsVeWMs7Xrbs+PPe/I5GMdGq9MSW/5TYbh
AOLaNb3XrmmhsPuRUuYgrdeT9wmCYhJBTRnO4U6e9owDIhypBNJxKxX6jx8fMiG6
bv1A591mN90XLjDmR8WzlTLUQszi2ELivQ/+qkkiBPiMkFX1BcZQfva4/UrqPL3d
Zo4XvFIBAoGBAOas4zRznaGjQ+s41Vw4ag4v6CyOuzqSKuHXhj/0NIY9Nh00shGm
o6Wbmk5e1ye570GnvdmDGG5m+CSHadTYJv2iOZuS5U+9SZKgyvskSRzRRmZbvm0Y
a4Bj0tEN38noOFxPSNytuHTP2BVYAE6r1rjF4ruuC9CkxoqlgOfcK6/xAoGBAOoX
wS4Knl/GV3FQDf+uYg6hCl/nRJ8jJeEqd4GgJQ6AOFYflo3A8mI4GgUVmwsfXCQF
QJTlQX3PzLsoKNmJupm5V2rsvxbd3Bf0uaqi/1v1KVg1LhKrgVfTSsoxTSN/zs7t
L2CeMPOkV+zG+Jd4daCaqUX9qM7RXN1BpLIlZ8MxAoGBAIZGm1db+aUuiYmyIpi1
Ch9KCcduKlSU4ztlnvM5bx25IffsRb96lgX/xtnZ4TpxxHEeeKfV5PHXJJj27kcE
r+WyegAyiSNalyowSRfZ7mlE9Mr+mqGctm8dLImenudEMxKg4FP9F8N/fVQnTr58
ztft8Oa5EI2abSwl3RyakB+xAoGBAJ5r9RBznzqxv/uvccnW+gVsdtjwakBISRpY
4weWmX12yKmANyM3sNFHplRKiuK8Fl9KLqqVe/zo5GTEiOCvlNjz1XVHQwr2mjyc
IgqYxLg28gP/+W4pIx+MntC3DL05gXwIiEeaxwYb8mULQrE/lFtcT7JOOC+/AFm8
Uvk81zwBAoGAJflMpjraVI1FpJ4stIBTtONPG/Ss+4n143yb3r1nRTBbayLhNp3h
uirQQi/CZAE1rlv1yg6iO4lRsH6BOLFaGjK7gEAPLug6ZEE+s1OjD07cHhmPo5q4
zr+qF4JvfzwLYt2X7PyLFjzkup26sXdkXFMxg7U9PtNvVrdGAod93ks=
-----END RSA PRIVATE KEY-----`

	caCert = `
-----BEGIN CERTIFICATE-----
MIIE9jCCAt6gAwIBAgIBATANBgkqhkiG9w0BAQsFADAbMRkwFwYDVQQDExBzY2Fs
YWJsZVN5c2xvZ0NBMB4XDTE5MDIwNjE3NDgwOVoXDTQ0MDIwNjE3NDgwOVowGzEZ
MBcGA1UEAxMQc2NhbGFibGVTeXNsb2dDQTCCAiIwDQYJKoZIhvcNAQEBBQADggIP
ADCCAgoCggIBAPtl5AEKm1W+uKr38FvEUhfoqQl6YaLysw/iBCXqjP33aJD4kvMm
BwYIcVavhMl8H1GSGC1HOaf1W0qGaNqnCGeoE0VPRC04cFTtz4TnZRHc1BSmahwY
63nuIqIuhha44w+31m4xTg0UZIXkpnTSFKCeXxWmD0bgpLrEdaRRplEXkK1LzFzT
CO37mQiW5C0jOsx2cQbWg21ma6j8GqqMju33kApiMfGncMY0lSFGeiXsxRuSqmDd
AeYyfa5c8xAQ5+hUx1k0u3RIg6ZCuOxl1uSTAFcI1vUEk9NtCEXrO4e51mUUKxsW
U7A4yIi/cTKamDOeWD25WixpEirO3tI52oyvsrj9Cob2FHjZJT/Ff7uEDYrRgnZW
X4roGu/J7WiyNkmUtdXjHj7jfhY4b0MVUX1zQb+JvzyEZsNYM0BKMzOqMfver/Vr
e5loVdGr+4e9E/39CLY1K0qtJZOzii9d/XQ43zRyZziyCUjn4CqnHjfGnwkZBkHN
UvT541sOwEAvaAcRkteM4rPUR/Qlh5lXIgxROjARoKmtr7wbQYq5icQT7NoaJCK4
qrp8H8Pk9IwgcvlhTrnB3tlW9dq/VBvvL/1xC0CaxR1+hFSdbWb8MVxZUkULTmWj
UGBdkD8ET0mGrHVBU0GJ/mlne9mOjpADubd+pGc419w3PYb36i9HQnPnAgMBAAGj
RTBDMA4GA1UdDwEB/wQEAwIBBjASBgNVHRMBAf8ECDAGAQH/AgEAMB0GA1UdDgQW
BBTnvdEqwGp+KoTK1L3vM4S48KSaGjANBgkqhkiG9w0BAQsFAAOCAgEArxTArpX3
b6m9uzkokgfgeq3AWjimOrczm5DB8SCsufs79e7soWYzeq1sYinGkJgqueogSTdW
g9wiho4X1c32D0rnfWugmQnXaGYz7Xa8/pram8ldlZkpa29Ac4xSttPKEbs8rcsQ
7yumpGPyxRWJ2o6GUNWSiiVfraUi8DN+l/Mr0PQ5uEzWLKoGFzsOCM+kGahjN1Ph
9L7B6mHRV8/qyFci6dz7QTw/43GVbYv5o5bNH1eO/jt0215sxmj7vF8SZ8o5SF3N
/52C6w+U0FGoORWyrrXQcxHl+InVn18BybuwQEXFk7BgX4Ch4wwPckEUom+qtUHy
xDZeLdP6VQesPGL/bQyQRkm9GIbWuVTrpZVNYrOtxzqaCA4HCrgrhLc+utzlLbT7
ZhNVefCNicO2ULBvV8WrNrXHumkd3JDWTKQY+BYNtCIdqgK9//3e7NFbA6P8xAlZ
ET+0XuWhhO5s2nvrygN8SiO5Zu/fZquKZPFZ7rAm8XBJIjW9PZVlk+E3kf9NbIcJ
CpuvPTVy7gUdbPw4u5fp8PTjGCYK3GoJwozanOgrzEvX/jrnxDrHr6pOCFHovSQ9
+GAWzqCCkH5ejZzXmjcG0F9YaLukaKGnu/q5PGWFg0E99xXAjXE8Deo3BRykyJ8Z
t3SEbEtNgGwYsbIr16S4NLT+jOXISirFfBA=
-----END CERTIFICATE-----`

	nonSignedCACert = `
-----BEGIN CERTIFICATE-----
MIIE6DCCAtCgAwIBAgIBATANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDEwlhbm90
aGVyQ0EwHhcNMTkwMjA2MTc0ODEwWhcNNDQwMjA2MTc0ODEwWjAUMRIwEAYDVQQD
Ewlhbm90aGVyQ0EwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQCezztN
lUcPgUJkNfqrgLYz6c2jA0kWYIiePQ7kWxyv718MVyiH2tA41iVcqVuhUfb1c2Ua
+18OR6xjwm1962so1XKIfjUqKzoGuLPIK3PJIydERa/B+p9lJTCPSnGROJvwFtLG
Ar09rGeySpuTg4D9+wyJEC/8YLFRHlZmS0dxJFrY8k9AZ5LxPgVATD1RJEzsoNyq
umQ7p/0V6CAm9MLgyH3oP50+Ei5ak5rES7xe3Vt3SRch/k5J97xGzF8wm+wxHlVN
F8WkqtmBQyNVHglDhWoWP+CX8n+9ZAjRYZty+lQtMW2gHHqQ+euA9IylNajSwDNc
oi/9PtQlRS947f4fdomthPRlSsw6j3ZRI2p6XvRpouJZ6uLN+7dBufefhL3eZBc8
1ztW3q1AIek1zF2kKS5Jg13UJKgbggpxeC8nZa0dutRRLDDfR9tSPWSy3DWtdDnO
87rjSja1Qi0n4swFvGJRNtGGuqqKn/QwwF9FU0YeWXcX2uS2awVwIIDsBrfpaHOp
utWsEiCUJXGyq8Pj3zu3gETirIh/2ml0wUwsiPs7cfK5P4bjThARpl8gj/J4HJV/
bwzwaoliU/xfPAJIJwRdfLgdql2HswsBN2Q5m+kFXdjMnE64YU0uID+3qrexo/ST
0EDJvL1KxmhMB+1oJE2+Or9b/lk4gwuQ2lU9WwIDAQABo0UwQzAOBgNVHQ8BAf8E
BAMCAQYwEgYDVR0TAQH/BAgwBgEB/wIBADAdBgNVHQ4EFgQUMtfdPLuaGnFb8l5Z
4egW3dv78o0wDQYJKoZIhvcNAQELBQADggIBABynLZ4TbWkbYm8Lr9UN8yZ7vJp5
NAmjK0GCq4yRRVtv6LNRQDk8Z0GB5/mjoosU/jGmkhUQ2NxDlMnePia7DfOrueKN
oBut6w3TuJK7U02MCnAmNhpKtS/VfuU00/AWWkTp0xFDNUPLsTz/AtM+k2iaHDVw
5RQR4zBv+OZH+DlxvUeOgDToZ03v+syg0a5HT4lM/7y7BpxX/q9sFSrzecNmpnId
Ix0qcTl1/foGPtY8mblmUuH007ewgwVOt5yS93srf0RTXzW6MWi3IFr1oABLqZIV
DxM2fFbYgqiMbhip0BAuadaMPi0Uv/k9oW36kVhSJeyIpFjAUJ8YUyxZrzkk5M/5
jRdiSMSyq0iJPtFa44k1MvbLgsMZ2MmKo4o1WXQ8ewMyw/JbxBgoxUvT1sFd+zOT
huDAWVkU9cciB69n/1ZsUs6idpRMcL/S054SEuRrULUSK83y1EGBHzVShQxhkRf4
04ygf/J5sP9K0CcdiP6B02gvCe5s1TC4qOw5GNrwJGWyfiFRxp0k+L2FzYKHAf25
z8+DYQ0Z97znKKuNy/hryZ2PiNaOAxCXvY3/4/JnmwqXGQJ7w+5x3MmEBWbZRSbG
wplijJuLiZNKfgjwIcWe5DQjUmuRooL1v9GJl7EeAhLBk4j38L7G4qdkShZQlaUy
58IHBdIBCA+5Qs5T
-----END CERTIFICATE-----
`
)
