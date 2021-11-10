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
	Address                net.IP
	Port                   int
	HostId                 uuid.UUID
	Datacenter             string
	Rack                   string
	ReleaseVersion         *string
	DseVersion             *optionalColumn
	Tokens                 []string
	SchemaVersion          *uuid.UUID
	Graph                  *optionalColumn
	JmxPort                *optionalColumn
	ServerId               *optionalColumn
	StoragePort            *optionalColumn
	StoragePortSsl         *optionalColumn
	Workload               *optionalColumn
	Workloads              *optionalColumn
	NativeTransportAddress *optionalColumn
	NativeTransportPort    *optionalColumn
	NativeTransportPortSsl *optionalColumn
}

func NewHost(
	address net.IP,
	port int,
	hostId uuid.UUID,
	datacenter string,
	rack string,
	releaseVersion *string,
	dseVersion *optionalColumn,
	tokens []string,
	schemaVersion *uuid.UUID,
	graph *optionalColumn,
	jmxPort *optionalColumn,
	serverId *optionalColumn,
	storagePort *optionalColumn,
	storagePortSsl *optionalColumn,
	workload *optionalColumn,
	workloads *optionalColumn,
	nativeTransportAddress *optionalColumn,
	nativeTransportPort *optionalColumn,
	nativeTransportPortSsl *optionalColumn) *Host {
	return &Host{
		Address:                address,
		Port:                   port,
		HostId:                 hostId,
		Datacenter:             datacenter,
		Rack:                   rack,
		ReleaseVersion:         releaseVersion,
		DseVersion:             dseVersion,
		Tokens:                 tokens,
		SchemaVersion:          schemaVersion,
		Graph:                  graph,
		JmxPort:                jmxPort,
		ServerId:               serverId,
		StoragePort:            storagePort,
		StoragePortSsl:         storagePortSsl,
		Workload:               workload,
		Workloads:              workloads,
		NativeTransportAddress: nativeTransportAddress,
		NativeTransportPort:    nativeTransportPort,
		NativeTransportPortSsl: nativeTransportPortSsl,
	}
}

func (recv *Host) String() string {
	return fmt.Sprintf("Host{addr: %v, port: %v, host_id: %v}",
		recv.Address,
		recv.Port,
		hex.EncodeToString(recv.HostId[:]))
}

func ParseSystemLocalResult(rs *ParsedRowSet, defaultPort int) (*systemLocalInfo, *Host, error) {
	if len(rs.Rows) < 1 {
		return nil, nil, fmt.Errorf("could not parse system local query result: query returned %d rows", len(rs.Rows))
	}

	if len(rs.Rows) > 1 {
		log.Warnf("system local query result returned %d rows", len(rs.Rows))
	}

	row := rs.Rows[0]

	addr, port, err := ParseRpcAddress(false, row, defaultPort)
	if err != nil {
		return nil, nil, err
	}

	host, err := parseHost(addr, port, row)
	if err != nil {
		return nil, nil, err
	}

	key := NewOptionalColumn(parseNillableString(row, "key"))
	bootstrapped := NewOptionalColumn(parseNillableString(row, "bootstrapped"))
	cqlVersion := NewOptionalColumn(parseNillableString(row, "cql_version"))
	gossipGeneration := NewOptionalColumn(parseNillableInt(row, "gossip_generation"))
	lastNodesyncCheckpoint := NewOptionalColumn(parseNillableBigInt(row, "last_nodesync_checkpoint_time"))
	nativeProtocolVersion := NewOptionalColumn(parseNillableString(row, "native_protocol_version"))
	partitioner := NewOptionalColumn(parseNillableString(row, "partitioner"))
	thriftVersion := NewOptionalColumn(parseNillableString(row, "thrift_version"))

	clusterName := NewOptionalColumn(parseNillableString(row, "cluster_name"))
	if !clusterName.exists || clusterName.column == nil {
		log.Warnf("could not get cluster_name using host %v", addr)
	}

	return &systemLocalInfo{
		key:                        key,
		clusterName:                clusterName,
		bootstrapped:               bootstrapped,
		cqlVersion:                 cqlVersion,
		gossipGeneration:           gossipGeneration,
		lastNodesyncCheckpointTime: lastNodesyncCheckpoint,
		nativeProtocolVersion:      nativeProtocolVersion,
		thriftVersion:              thriftVersion,
		partitioner:                partitioner,
	}, host, nil
}

func ParseSystemPeersResult(rs *ParsedRowSet, defaultPort int, isPeersV2 bool) (hosts map[uuid.UUID]*Host, preferredIpColExists bool) {
	hosts = make(map[uuid.UUID]*Host)
	first := true
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

		if first {
			first = false
			_, preferredIpColExists = row.GetByColumn("preferred_ip")
		}

		oldHost, hostExists := hosts[host.HostId]
		if hostExists {
			log.Warnf("Duplicate host found: %v vs %v. Ignoring the former one.", oldHost, host)
		}
		hosts[host.HostId] = host
	}

	return hosts, preferredIpColExists
}

func parseHost(addr net.IP, port int, row *ParsedRow) (*Host, error) {
	datacenter, err := parseString(row, "data_center")
	if err != nil {
		return nil, fmt.Errorf("could not parse data_center of host %v: %w", addr, err)
	}

	rack, err := parseString(row, "rack")
	if err != nil {
		return nil, fmt.Errorf("could not parse rack of host %v: %w", addr, err)
	}

	tokens, _ := parseNillableStringSlice(row, "tokens")

	releaseVersion, _ := parseNillableString(row, "release_version")
	if releaseVersion == nil {
		log.Warnf("could not parse release_version of host %v", addr)
	}

	dseVersion := NewOptionalColumn(parseNillableString(row, "dse_version"))

	hostId, _, err := parseNillableUuid(row, "host_id")
	if hostId == nil {
		if err != nil {
			return nil, fmt.Errorf("could not parse host_id for host %v: %w", addr, err)
		} else {
			return nil, fmt.Errorf("host_id for host %v is nil", addr)
		}
	}

	schemaId, _, err := parseNillableUuid(row, "schema_version")
	if schemaId == nil {
		if err != nil {
			log.Warnf("could not parse schema_version for host %v: %v", addr, err)
		} else {
			log.Warnf("schema_version for host %v is nil", addr)
		}
	}

	graph := NewOptionalColumn(parseNillableBool(row, "graph"))
	jmxPort := NewOptionalColumn(parseNillableInt(row, "jmx_port"))
	serverId := NewOptionalColumn(parseNillableString(row, "server_id"))
	storagePort := NewOptionalColumn(parseNillableInt(row, "storage_port"))
	storagePortSsl := NewOptionalColumn(parseNillableInt(row, "storage_port_ssl"))
	workload := NewOptionalColumn(parseNillableString(row, "workload"))
	workloads := NewOptionalColumn(parseNillableStringSlice(row, "workloads"))
	nativeTransportAddress := NewOptionalColumn(row.GetByColumn("native_transport_address"))
	nativeTransportPort := NewOptionalColumn(parseNillableInt(row, "native_transport_port"))
	nativeTransportPortSsl := NewOptionalColumn(parseNillableInt(row, "native_transport_port_ssl"))

	return NewHost(
		addr,
		port,
		*hostId,
		datacenter,
		rack,
		releaseVersion,
		dseVersion,
		tokens,
		schemaId,
		graph,
		jmxPort,
		serverId,
		storagePort,
		storagePortSsl,
		workload,
		workloads,
		nativeTransportAddress,
		nativeTransportPort,
		nativeTransportPortSsl), nil
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

		log.Infof("Found host with 0.0.0.0 as rpc_address, using listen_address (%v) to contact it instead. " +
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
		port, ok := val.(int32)
		return int(port), ok
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

func parseNillableStringSlice(row *ParsedRow, column string) ([]string, bool) {
	var slice []*string
	val, ok := row.GetByColumn(column)
	if val == nil {
		return nil, ok
	}

	slice = val.([]*string)
	strSlice := make([]string, 0, len(slice))
	for _, elem := range slice {
		if elem != nil {
			strSlice = append(strSlice, *elem)
		}
	}

	return strSlice, true
}

func parseNillableString(row *ParsedRow, column string) (*string, bool) {
	val, exists := row.GetByColumn(column)
	if val == nil {
		return nil, exists
	}

	output := val.(string)
	return &output, true
}

func parseString(row *ParsedRow, column string) (string, error) {
	val, exists := parseNillableString(row, column)
	if !exists {
		return "", fmt.Errorf("could not find column %v", column)
	}

	if val == nil {
		return "", fmt.Errorf("column %v is nil", column)
	}

	return *val, nil
}

func parseNillableInt(row *ParsedRow, column string) (*int32, bool) {
	val, exists := row.GetByColumn(column)
	if val == nil {
		return nil, exists
	}

	output := val.(int32)
	return &output, true
}

func parseNillableBigInt(row *ParsedRow, column string) (*int64, bool) {
	val, exists := row.GetByColumn(column)
	if val == nil {
		return nil, exists
	}

	output := val.(int64)
	return &output, true
}

func parseNillableBool(row *ParsedRow, column string) (*bool, bool) {
	val, exists := row.GetByColumn(column)
	if val == nil {
		return nil, exists
	}

	output := val.(bool)
	return &output, true
}

func parseNillableUuid(row *ParsedRow, column string) (*uuid.UUID, bool, error) {
	var parsedUuid uuid.UUID
	var primitiveUuid primitive.UUID
	val, ok := row.GetByColumn(column)
	if val == nil {
		return nil, ok, nil
	}

	primitiveUuid, ok = val.(primitive.UUID)
	if !ok {
		return nil, true, fmt.Errorf("could not convert %v %v to primitive.UUID", column, val)
	}

	var err error
	parsedUuid, err = uuid.FromBytes(primitiveUuid[:])
	if err != nil {
		return nil, true, fmt.Errorf("could not convert %v %v from primitive.UUID to uuid.UUID", column, val)
	}

	return &parsedUuid, true, nil
}