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
	SourceIndex          string

	MetricIngressAddr     string
	MetricIngressCN       string
	MetricEmitterInterval time.Duration
}

func LoadConfig() *Config {
	var cfg Config

	flag.StringVar(&cfg.HealthHostport, "health", ":8080", "The hostport to listen for health requests")
	flag.StringVar(&cfg.AdapterHostport, "addr", ":4443", "The hostport to for the adapter server")
	flag.StringVar(&cfg.PprofHostport, "pprof", "localhost:6060", "The hostport to listen for pprof")

	flag.StringVar(&cfg.CAFile, "ca", "", "The file path for the CA cert")
	flag.StringVar(&cfg.CertFile, "cert", "", "The file path for the adapter server cert")
	flag.StringVar(&cfg.KeyFile, "key", "", "The file path for the adapter server key")
	flag.StringVar(&cfg.CommonName, "cn", "", "The common name used for the TLS config")

	flag.StringVar(&cfg.RLPCAFile, "rlp-ca", "", "The file path for the Loggregator CA cert")
	flag.StringVar(&cfg.RLPCertFile, "rlp-cert", "", "The file path for the adapter RLP client cert")
	flag.StringVar(&cfg.RLPKeyFile, "rlp-key", "", "The file path for the adapter RLP client key")
	flag.StringVar(&cfg.RLPCommonName, "rlp-cn", "", "The common name for the Loggregator egress API")

	flag.DurationVar(&cfg.SyslogDialTimeout, "syslog-dial-timeout", 5*time.Second, "The timeout for dialing to syslog drains")
	flag.DurationVar(&cfg.SyslogIOTimeout, "syslog-io-timeout", time.Minute, "The timeout for writing to syslog drains")
	flag.BoolVar(&cfg.SyslogSkipCertVerify, "syslog-skip-cert-verify", false, "The option to not verify syslog TLS certs")

	flag.StringVar(&cfg.SourceIndex, "source-index", "", "The given index for the adapter")

	flag.StringVar(&cfg.LogsAPIAddr, "logs-api-addr", "", "The address for the logs API")
	flag.StringVar(&cfg.MetricIngressAddr, "metric-ingress-addr", "", "The ingress adress for the metrics API")
	flag.StringVar(&cfg.MetricIngressCN, "metric-ingress-cn", "", "The TLS common name for metrics ingress API")
	flag.DurationVar(&cfg.MetricEmitterInterval, "metric-emitter-interval", time.Minute, "The interval to send batched metrics to metron")

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
