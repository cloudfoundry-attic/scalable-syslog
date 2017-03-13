package ingress

//go:generate hel --type ClientPool,ReceiverClient,SyslogConnector

import v2 "github.com/cloudfoundry-incubator/scalable-syslog/api/loggregator/v2"

type ReceiverClient interface {
	v2.Egress_ReceiverClient
}
