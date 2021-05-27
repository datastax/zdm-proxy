package cloudgateproxy

import (
	"encoding/hex"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"net"
)

type Host struct {
	Address        net.IP
	Port           int
	HostId         uuid.UUID
	Datacenter     string
	Rack           string
	ReleaseVersion string
	DseVersion     string
	Tokens         []string
}

func NewHost(
	address net.IP,
	port int,
	hostId uuid.UUID,
	datacenter string,
	rack string,
	releaseVersion string,
	dseVersion string,
	tokens []string) *Host {
	return &Host{
		Address:        address,
		Port:           port,
		HostId:         hostId,
		Datacenter:     datacenter,
		Rack:           rack,
		ReleaseVersion: releaseVersion,
		DseVersion:     dseVersion,
		Tokens:         tokens,
	}
}

func (recv *Host) String() string {
	return fmt.Sprintf("Host{addr: %v, port: %v, host_id: %v, dc: %v, rack: %v, tokens: %v, version: %v, dse_version: %v",
		recv.Address,
		recv.Port,
		hex.EncodeToString(recv.HostId[:]),
		recv.Datacenter,
		recv.Rack,
		recv.Tokens,
		recv.ReleaseVersion,
		recv.DseVersion)
}

func ParseSystemLocalResult(rs *ParsedRowSet, defaultPort int) (*Host, string, error) {
	if len(rs.Rows) < 1 {
		return nil, "", fmt.Errorf("could not parse system local query result: query returned %d rows", len(rs.Rows))
	}

	if len(rs.Rows) > 1 {
		log.Warnf("system local query result returned %d rows", len(rs.Rows))
	}

	row := rs.Rows[0]

	addr, port, err := ParseRpcAddress(false, row, defaultPort)
	if err != nil {
		return nil, "", err
	}

	host, err := parseHost(addr, port, row)
	if err != nil {
		return nil, "", err
	}

	var clusterName string
	val, ok := row.GetByColumn("cluster_name")
	if !ok {
		log.Warnf("could not get cluster_name using host %v", addr)
		clusterName = ""
	} else {
		clusterName = val.(string)
	}

	return host, clusterName, nil
}

func ParseSystemPeersResult(rs *ParsedRowSet, defaultPort int, isPeersV2 bool) []*Host {
	hosts := make([]*Host, 0)
	for _, row := range rs.Rows {
		addr, port, err := ParseRpcAddress(isPeersV2, row, defaultPort)
		if err != nil {
			log.Warnf("error parsing peer host address, skipping it: %v", err)
			continue
		}

		host, err := parseHost(addr, port, row)
		if err != nil {
			log.Warnf("error parsing information of peer host %v:%d, skipping it: %v", addr, port, err)
			continue
		}

		hosts = append(hosts, host)
	}

	return hosts
}

func parseHost(addr net.IP, port int, row *ParsedRow) (*Host, error) {
	var datacenter string
	val, ok := row.GetByColumn("data_center")
	if !ok {
		return nil, fmt.Errorf("could not parse data_center of host %v", addr)
	}
	datacenter = val.(string)

	var rack string
	val, ok = row.GetByColumn("rack")
	if !ok {
		return nil, fmt.Errorf("could not parse rack of host %v", addr)
	}
	rack = val.(string)

	var tokensSlice []interface{}
	val, ok = row.GetByColumn("tokens")
	if !ok {
		return nil, fmt.Errorf("could not parse tokens of host %v", addr)
	}
	tokensSlice = val.([]interface{})
	tokens := make([]string, len(tokensSlice))
	for idx, token := range tokensSlice {
		tokens[idx], ok = token.(string)
		if !ok {
			return nil, fmt.Errorf("decoded token of host %v is %T instead of string", addr, token)
		}
	}

	var releaseVersion string
	val, ok = row.GetByColumn("release_version")
	if !ok {
		log.Warnf("could not parse release_version of host %v", addr)
		releaseVersion = ""
	} else {
		releaseVersion = val.(string)
	}

	var dseVersion string
	val, ok = row.GetByColumn("dse_version")
	if !ok {
		dseVersion = ""
	} else {
		dseVersion = val.(string)
	}

	var parsedHostId uuid.UUID
	var hostId primitive.UUID
	val, ok = row.GetByColumn("host_id")
	if !ok || val == nil {
		return nil, fmt.Errorf("could not parse host_id of host %v", addr)
	} else {
		hostId, ok = val.(primitive.UUID)
		if !ok {
			return nil, fmt.Errorf("could not convert host id %v to primitive.UUID for host %v", val, addr)
		}

		var err error
		parsedHostId, err = uuid.FromBytes(hostId[:])
		if err != nil {
			return nil, fmt.Errorf("could not convert host id %v from primitive.UUID to uuid.UUID for host %v", val, addr)
		}
	}

	return NewHost(
		addr,
		port,
		parsedHostId,
		datacenter,
		rack,
		releaseVersion,
		dseVersion,
		tokens), nil
}

func ParseRpcAddress(isPeersV2 bool, row *ParsedRow, defaultPort int) (net.IP, int, error) {
	var addr net.IP

	if isPeersV2 {
		addr = parseRpcAddressPeersV2(row)
	} else {
		addr = parseRpcAddressLocalOrPeersV1(row)
	}

	if addr.IsUnspecified() {
		peer, peerExists := row.GetByColumn("peer")
		if peerExists && peer != nil {
			// system.peers
			addr, _ = peer.(net.IP)
		} else if bcastaddr, bcastaddrExists := row.GetByColumn("broadcast_address"); bcastaddrExists && bcastaddr != nil {
			// system.local
			addr, _ = bcastaddr.(net.IP)
		} else if listenAddr, listenAddrExists := row.GetByColumn("listen_address"); listenAddrExists && listenAddr != nil {
			// system.local
			addr, _ = bcastaddr.(net.IP)
		} else {
			return nil, -1, fmt.Errorf(
				"found host with 0.0.0.0 as rpc_address and nulls as listen_address and broadcast_address; " +
				"because of this, the proxy can not connect to this node")
		}

		log.Warnf("Found host with 0.0.0.0 as rpc_address, using listen_address (%v) to contact it instead. " +
			"If this is incorrect you should avoid the use of 0.0.0.0 server side.", addr)
	}

	rpcPort := defaultPort
	if isPeersV2 {
		val, ok := parseRpcPortPeersV2(row)
		if !ok {
			log.Warnf(
				"Found host with NULL native_port, using default port (%v) to contact it instead.", rpcPort)
		} else {
			rpcPort = val
		}
	}

	return addr, rpcPort, nil
}

func parseRpcPortPeersV2(row *ParsedRow) (int, bool) {
	val, ok := row.GetByColumn("native_port")
	if ok && val != nil {
		port, ok := val.(int)
		return port, ok
	}

	return -1, false
}

func parseRpcAddressLocalOrPeersV1(row *ParsedRow) net.IP {
	return parseAddress(row, "rpc_address")
}

func parseRpcAddressPeersV2(row *ParsedRow) net.IP {
	return parseAddress(row, "native_address")
}

func parseAddress(row *ParsedRow, columnName string) net.IP {
	val, ok := row.GetByColumn(columnName)
	if ok {
		ip, _ := val.(net.IP)
		return ip
	}

	return nil
}