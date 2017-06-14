package ingress

//go:generate hel --type ClientPool,ReceiverClient,SyslogConnector

import v2 "code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"

type ReceiverClient interface {
	v2.Egress_ReceiverClient
}
