package app

import (
	"log"
	"strings"
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"golang.org/x/net/idna"
)

type Config struct {
	SourceIndex          string        `env:"ADAPTER_INSTANCE_INDEX,  required"`
	HealthHostport       string        `env:"HEALTH_HOSTPORT,         required"`
	AdapterHostport      string        `env:"HOSTPORT,                required"`
	PprofHostport        string        `env:"PPROF_HOSTPORT,          required"`
	CAFile               string        `env:"CA_FILE_PATH,            required"`
	CertFile             string        `env:"CERT_FILE_PATH,          required"`
	KeyFile              string        `env:"KEY_FILE_PATH,           required"`
	CommonName           string        `env:"TLS_COMMON_NAME,         required"`
	RLPCAFile            string        `env:"LOGS_API_CA_FILE,        required"`
	RLPCertFile          string        `env:"LOGS_API_CERT_FILE_PATH, required"`
	RLPKeyFile           string        `env:"LOGS_API_KEY_FILE_PATH,  required"`
	RLPCommonName        string        `env:"LOGS_API_COMMON_NAME,    required"`
	LogsAPIAddr          string        `env:"LOGS_API_ADDR,           required"`
	LogsAPIAddrWithAZ    string        `env:"LOGS_API_ADDR_WITH_AZ,   required"`
	SyslogDialTimeout    time.Duration `env:"SYSLOG_DIAL_TIMEOUT,     required"`
	SyslogIOTimeout      time.Duration `env:"SYSLOG_IO_TIMEOUT,       required"`
	SyslogSkipCertVerify bool          `env:"SYSLOG_SKIP_CERT_VERIFY, required"`

	MetricIngressAddr     string        `env:"METRIC_INGRESS_ADDR,     required"`
	MetricIngressCN       string        `env:"METRIC_INGRESS_CN,       required"`
	MetricEmitterInterval time.Duration `env:"METRIC_EMITTER_INTERVAL, required"`
}

func LoadConfig() *Config {
	cfg := Config{
		HealthHostport:        ":8080",
		AdapterHostport:       ":4443",
		PprofHostport:         "localhost:6060",
		SyslogDialTimeout:     5 * time.Second,
		SyslogIOTimeout:       time.Minute,
		SyslogSkipCertVerify:  false,
		MetricEmitterInterval: time.Minute,
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
