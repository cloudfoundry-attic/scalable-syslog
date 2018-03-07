package app

import (
	"log"
	"strings"
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"golang.org/x/net/idna"
)

// Config stores configuration settings for the adapter.
type Config struct {
	SourceIndex            string        `env:"ADAPTER_INSTANCE_INDEX,  required"`
	CAFile                 string        `env:"CA_FILE_PATH,            required"`
	CertFile               string        `env:"CERT_FILE_PATH,          required"`
	KeyFile                string        `env:"KEY_FILE_PATH,           required"`
	CommonName             string        `env:"TLS_COMMON_NAME,         required"`
	RLPCAFile              string        `env:"LOGS_API_CA_FILE,        required"`
	RLPCertFile            string        `env:"LOGS_API_CERT_FILE_PATH, required"`
	RLPKeyFile             string        `env:"LOGS_API_KEY_FILE_PATH,  required"`
	RLPCommonName          string        `env:"LOGS_API_COMMON_NAME,    required"`
	LogsAPIAddr            string        `env:"LOGS_API_ADDR,           required"`
	LogsAPIAddrWithAZ      string        `env:"LOGS_API_ADDR_WITH_AZ,   required"`
	HealthHostport         string        `env:"HEALTH_HOSTPORT"`
	AdapterHostport        string        `env:"HOSTPORT"`
	PprofHostport          string        `env:"PPROF_HOSTPORT"`
	SyslogKeepalive        time.Duration `env:"SYSLOG_KEEPALIVE"`
	SyslogDialTimeout      time.Duration `env:"SYSLOG_DIAL_TIMEOUT"`
	SyslogIOTimeout        time.Duration `env:"SYSLOG_IO_TIMEOUT"`
	SyslogSkipCertVerify   bool          `env:"SYSLOG_SKIP_CERT_VERIFY"`
	MetricsToSyslogEnabled bool          `env:"METRICS_TO_SYSLOG_ENABLED"`
	MaxBindings            int           `env:"MAX_BINDINGS"`

	MetricIngressAddr     string        `env:"METRIC_INGRESS_ADDR,     required"`
	MetricIngressCN       string        `env:"METRIC_INGRESS_CN,       required"`
	MetricEmitterInterval time.Duration `env:"METRIC_EMITTER_INTERVAL"`
}

// LoadConfig will load and validate the config from the current environment.
// If validation fails LoadConfig will log the error and exit the process with
// status code 1.
func LoadConfig() *Config {
	cfg := Config{
		HealthHostport:         ":8080",
		AdapterHostport:        ":4443",
		PprofHostport:          "localhost:6060",
		SyslogDialTimeout:      5 * time.Second,
		SyslogIOTimeout:        time.Minute,
		SyslogSkipCertVerify:   false,
		MetricEmitterInterval:  time.Minute,
		MetricsToSyslogEnabled: false,
		MaxBindings:            500,
	}

	err := envstruct.Load(&cfg)
	if err != nil {
		log.Fatalf("failed to load config from environment: %s", err)
	}

	cfg.LogsAPIAddrWithAZ, err = idna.ToASCII(cfg.LogsAPIAddrWithAZ)
	if err != nil {
		log.Fatalf("failed to IDN encode LogAPIAddrWithAZ %s", err)
	}
	cfg.LogsAPIAddrWithAZ = strings.Replace(cfg.LogsAPIAddrWithAZ, "@", "-", -1)

	return &cfg
}
