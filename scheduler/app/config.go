package app

import (
	"fmt"
	"log"
	"net"
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/scalable-syslog/scheduler/internal/ingress"
)

// Config stores configuration settings for the scheduler.
type Config struct {
	APIURL             string        `env:"API_URL,              required"`
	APICAFile          string        `env:"API_CA_FILE_PATH,     required"`
	APICertFile        string        `env:"API_CERT_FILE_PATH,   required"`
	APIKeyFile         string        `env:"API_KEY_FILE_PATH,    required"`
	APICommonName      string        `env:"API_COMMON_NAME,      required"`
	APISkipCertVerify  bool          `env:"API_SKIP_CERT_VERIFY"`
	APIPollingInterval time.Duration `env:"API_POLLING_INTERVAL"`
	APIBatchSize       int           `env:"API_BATCH_SIZE"`

	CAFile            string `env:"CA_FILE_PATH,        required"`
	CertFile          string `env:"CERT_FILE_PATH,      required"`
	KeyFile           string `env:"KEY_FILE_PATH,       required"`
	AdapterCommonName string `env:"ADAPTER_COMMON_NAME, required"`

	Blacklist *ingress.BlacklistRanges `env:"BLACKLIST"`

	AdapterPort  string   `env:"ADAPTER_PORT,  required"`
	AdapterAddrs []string `env:"ADAPTER_ADDRS, required"`

	MetricIngressAddr     string        `env:"METRIC_INGRESS_ADDR, required"`
	MetricIngressCN       string        `env:"METRIC_INGRESS_CN,   required"`
	MetricEmitterInterval time.Duration `env:"METRIC_EMITTER_INTERVAL"`
	HealthHostport        string        `env:"HEALTH_HOSTPORT"`
	PprofHostport         string        `env:"PPROF_HOSTPORT"`
}

// LoadConfig will load and validate the config from the current environment.
// If validation fails LoadConfig will log the error and exit the process with
// status code 1.
func LoadConfig(args []string) (*Config, error) {
	cfg := Config{
		HealthHostport:        ":8080",
		PprofHostport:         "localhost:6060",
		APISkipCertVerify:     false,
		APIPollingInterval:    15 * time.Second,
		MetricEmitterInterval: time.Minute,
		Blacklist:             &ingress.BlacklistRanges{},
		APIBatchSize:          1000,
	}

	if err := envstruct.Load(&cfg); err != nil {
		log.Fatalf("failed to load config from environment: %s", err)
	}

	hostports, err := resolveAddrs(cfg.AdapterAddrs, cfg.AdapterPort)
	if err != nil {
		log.Fatalf("failed to resolve adapter addrs: %s", err)
	}
	cfg.AdapterAddrs = hostports

	return &cfg, nil
}

// resolveAddrs does two things:
// 1. Does a DNS lookup of the addresses to ensure they are valid.
// 2. Adds the given port to create hostport.
func resolveAddrs(hosts []string, port string) ([]string, error) {
	var hostports []string
	for _, h := range hosts {
		resolved, err := net.LookupIP(h)
		if err != nil {
			return nil, err
		}

		for _, h := range resolved {
			hostports = append(hostports, fmt.Sprintf("%s:%s", h, port))
		}
	}

	return hostports, nil
}
