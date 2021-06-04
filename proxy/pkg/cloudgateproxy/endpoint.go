package cloudgateproxy

import (
	"fmt"
)

type EndpointConfig interface {
	getEndpoint() string
	isSNI()	bool
}

type SniEndpointConfig struct {
	hostId	string		//this is the host ID and is only populated when SNI is being used
	port 	int
}

type DirectEndpointConfig struct {
	host	string	// can be a hostname or an IP address (no need to distinguish at this time, as hostnames are resolved automatically)
	port		int
}

func (sec SniEndpointConfig) getEndpoint() string {
	return sec.hostId
}

func (sec SniEndpointConfig) isSNI() bool{
	return true
}

func (dec DirectEndpointConfig) getEndpoint() string {
	return fmt.Sprintf("%s:%d", dec.host, dec.port)
}

func (dec DirectEndpointConfig) isSNI() bool{
	return false
}

/* For SNI:
    - address is going to be ConnectionConfig.SniProxyAddress
    - port is going to be ConnectionConfig.cqlPort
   For direct connection:
    - address is going to be p.Conf.OriginCassandraHost (or target)
    - port is going to be p.Conf.OriginCassandraPort (or target)
*/
func NewEndpointConfig(host string, isSni bool, port int) EndpointConfig {
	if isSni {
		return SniEndpointConfig{
			hostId: host,
			port:   port,
		}
	} else {
		return DirectEndpointConfig{
			host: host,
			port: port,
		}
	}
}

