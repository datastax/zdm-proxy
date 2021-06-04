package cloudgateproxy

import (
	"crypto/tls"
)

type SNIConfig struct {
	SNIProxyAddress string
	tlsConfig		*tls.Config
}

type SNIControlConnection struct {
	controlConn ControlConn
	sniConfig SNIConfig
}
