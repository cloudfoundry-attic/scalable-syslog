package app

import (
	"flag"
	"fmt"
	"log"
	"time"
)

type Config struct {
	HealthHostport       string
	AdapterHostport      string
	PprofHostport        string
	CAFile               string
	CertFile             string
	KeyFile              string
	CommonName           string
	RLPCAFile            string
	RLPCertFile          string
	RLPKeyFile           string
	RLPCommonName        string
	LogsAPIAddr          string
	SyslogDialTimeout    time.Duration
	SyslogIOTimeout      time.Duration
	SyslogSkipCertVerify bool
}

func LoadConfig() *Config {
	var cfg Config

	flag.StringVar(&cfg.HealthHostport, "health", ":8080", "The hostport to listen for health requests")
	flag.StringVar(&cfg.AdapterHostport, "addr", ":4443", "The hostport to for the adapter controller")
	flag.StringVar(&cfg.PprofHostport, "pprof", "localhost:6060", "The hostport to listen for pprof")

	flag.StringVar(&cfg.CAFile, "ca", "", "The file path for the CA cert")
	flag.StringVar(&cfg.CertFile, "cert", "", "The file path for the adapter server cert")
	flag.StringVar(&cfg.KeyFile, "key", "", "The file path for the adapter server key")
	flag.StringVar(&cfg.CommonName, "cn", "", "The common name used for the TLS config")

	flag.StringVar(&cfg.RLPCAFile, "rlp-ca", "", "The file path for the Loggregator CA cert")
	flag.StringVar(&cfg.RLPCertFile, "rlp-cert", "", "The file path for the adapter RLP client cert")
	flag.StringVar(&cfg.RLPKeyFile, "rlp-key", "", "The file path for the adapter RLP client key")
	flag.StringVar(&cfg.RLPCommonName, "rlp-cn", "", "The common name for the Loggregator egress API")

	flag.DurationVar(&cfg.SyslogDialTimeout, "syslog-dial-timeout", time.Second, "The timeout for dialing to syslog drains")
	flag.DurationVar(&cfg.SyslogIOTimeout, "syslog-io-timeout", 60*time.Second, "The timeout for writing to syslog drains")
	flag.BoolVar(&cfg.SyslogSkipCertVerify, "syslog-skip-cert-verify", true, "The option to not verify syslog TLS certs")

	flag.StringVar(&cfg.LogsAPIAddr, "logs-api-addr", "", "The address for the logs API")
	flag.Parse()

	var errs []error
	flag.VisitAll(func(f *flag.Flag) {
		if f.Value.String() == "" {
			errs = append(errs, fmt.Errorf("Missing required flag %s", f.Name))
		}
	})

	if len(errs) != 0 {
		var errorMsg string
		for _, e := range errs {
			errorMsg += fmt.Sprintf("  %s\n", e.Error())
		}
		log.Fatalf("Config validation failed:\n%s", errorMsg)
	}

	return &cfg
}
