package config

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
)

type Config struct {
	HealthHostport    string
	PprofHostport     string
	APIURL            string
	APICAFile         string
	APICertFile       string
	APIKeyFile        string
	APICommonName     string
	APISkipCertVerify bool
	CAFile            string
	CertFile          string
	KeyFile           string
	AdapterCommonName string
	AdapterIPs        string
	AdapterPort       string
	AdapterAddrs      []string
}

func Load() *Config {
	var cfg Config

	flag.StringVar(&cfg.HealthHostport, "health", ":8080", "The hostport to listen for health requests")
	flag.StringVar(&cfg.PprofHostport, "pprof", ":6060", "The hostport to listen for pprof")

	flag.StringVar(&cfg.APIURL, "api-url", "", "The URL of the binding provider")
	flag.StringVar(&cfg.APICAFile, "api-ca", "", "The file path for the CA cert")
	flag.StringVar(&cfg.APICertFile, "api-cert", "", "The file path for the client cert")
	flag.StringVar(&cfg.APIKeyFile, "api-key", "", "The file path for the client key")
	flag.StringVar(&cfg.APICommonName, "api-cn", "", "The common name used for the TLS config")
	flag.BoolVar(&cfg.APISkipCertVerify, "api-skip-cert-verify", false, "The option to allow insecure SSL connections")

	flag.StringVar(&cfg.CAFile, "ca", "", "The file path for the CA cert")
	flag.StringVar(&cfg.CertFile, "cert", "", "The file path for the adapter server cert")
	flag.StringVar(&cfg.KeyFile, "key", "", "The file path for the adapter server key")

	flag.StringVar(&cfg.AdapterCommonName, "adapter-cn", "", "The common name used for the TLS config")
	flag.StringVar(&cfg.AdapterPort, "adapter-port", "", "The port of the adapter API")
	flag.StringVar(&cfg.AdapterIPs, "adapter-ips", "", "Comma separated list of adapter IP addresses")

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

	var err error
	cfg.AdapterAddrs, err = parseAddrs(cfg.AdapterIPs, cfg.AdapterPort)
	if err != nil {
		log.Fatalf("No adapter addresses: %s", err)
	}

	return &cfg
}

func parseAddrs(ips, port string) ([]string, error) {
	var hostports []string

	if len(ips) == 0 {
		return nil, errors.New("no IP addresses provided")
	}

	hosts := strings.Split(ips, ",")

	for _, h := range hosts {
		if net.ParseIP(h) == nil {
			return nil, fmt.Errorf("invalid IP format: %s", h)
		}
		hp := fmt.Sprintf("%s:%s", h, port)
		hostports = append(hostports, hp)
	}

	return hostports, nil
}
