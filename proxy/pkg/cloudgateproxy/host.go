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
	Address       net.IP
	Port          int
	HostId        uuid.UUID
	Datacenter    string
	Rack          string
	Tokens        []string
	SchemaVersion *uuid.UUID
	ColumnData    map[string]*optionalColumn
}

func NewHost(
	address net.IP,
	port int,
	hostId uuid.UUID,
	datacenter string,
	rack string,
	tokens []string,
	schemaVersion *uuid.UUID,
	columnData map[string]*optionalColumn) *Host {
	return &Host{
		Address:       address,
		Port:          port,
		HostId:        hostId,
		Datacenter:    datacenter,
		Rack:          rack,
		Tokens:        tokens,
		SchemaVersion: schemaVersion,
		ColumnData:    columnData,
	}
}

func (recv *Host) String() string {
	return fmt.Sprintf("Host{addr: %v, port: %v, host_id: %v}",
		recv.Address,
		recv.Port,
		hex.EncodeToString(recv.HostId[:]))
}

func ParseSystemLocalResult(rs *ParsedRowSet, defaultPort int) (map[string]*optionalColumn, *Host, error) {
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

	sysLocalCols := map[string]*optionalColumn{
		keyColumn.Name:                        NewOptionalColumn(parseNillableString(row, keyColumn.Name)),
		bootstrappedColumn.Name:               NewOptionalColumn(parseNillableString(row, bootstrappedColumn.Name)),
		cqlVersionColumn.Name:                 NewOptionalColumn(parseNillableString(row, cqlVersionColumn.Name)),
		gossipGenerationColumn.Name:           NewOptionalColumn(parseNillableInt(row, gossipGenerationColumn.Name)),
		lastNodesyncCheckpointTimeColumn.Name: NewOptionalColumn(parseNillableBigInt(row, lastNodesyncCheckpointTimeColumn.Name)),
		nativeProtocolVersionColumn.Name:      NewOptionalColumn(parseNillableString(row, nativeProtocolVersionColumn.Name)),
		partitionerColumn.Name:                NewOptionalColumn(parseNillableString(row, partitionerColumn.Name)),
		thriftVersionColumn.Name:              NewOptionalColumn(parseNillableString(row, thriftVersionColumn.Name)),
		clusterNameColumn.Name:                NewOptionalColumn(parseNillableString(row, clusterNameColumn.Name)),
	}

	if clusterName, exists := sysLocalCols[clusterNameColumn.Name]; !exists || clusterName.column == nil {
		log.Warnf("could not get %v using host %v", clusterNameColumn.Name, addr)
	}

	return sysLocalCols, host, nil
}

func ParseSystemPeersResult(rs *ParsedRowSet, defaultPort int, isPeersV2 bool) map[uuid.UUID]*Host {
	hosts := make(map[uuid.UUID]*Host)
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

		oldHost, hostExists := hosts[host.HostId]
		if hostExists {
			log.Warnf("Duplicate host found: %v vs %v. Ignoring the former one.", oldHost, host)
		}
		hosts[host.HostId] = host
	}

	return hosts
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

	columnData := map[string]*optionalColumn{
		releaseVersionPeersColumn.Name:         NewOptionalColumn(parseNillableString(row, releaseVersionPeersColumn.Name)),
		dseVersionPeersColumn.Name:             NewOptionalColumn(parseNillableString(row, dseVersionPeersColumn.Name)),
		graphPeersColumn.Name:                  NewOptionalColumn(parseNillableBool(row, graphPeersColumn.Name)),
		jmxPortPeersColumn.Name:                NewOptionalColumn(parseNillableInt(row, jmxPortPeersColumn.Name)),
		serverIdPeersColumn.Name:               NewOptionalColumn(parseNillableString(row, serverIdPeersColumn.Name)),
		storagePortPeersColumn.Name:            NewOptionalColumn(parseNillableInt(row, storagePortPeersColumn.Name)),
		storagePortSslPeersColumn.Name:         NewOptionalColumn(parseNillableInt(row, storagePortSslPeersColumn.Name)),
		workloadPeersColumn.Name:               NewOptionalColumn(parseNillableString(row, workloadPeersColumn.Name)),
		workloadsPeersColumn.Name:              NewOptionalColumn(parseNillableStringSlice(row, workloadsPeersColumn.Name)),
		nativeTransportAddressPeersColumn.Name: NewOptionalColumn(row.GetByColumn(nativeTransportAddressPeersColumn.Name)),
		nativeTransportPortPeersColumn.Name:    NewOptionalColumn(parseNillableInt(row, nativeTransportPortPeersColumn.Name)),
		nativeTransportPortSslPeersColumn.Name: NewOptionalColumn(parseNillableInt(row, nativeTransportPortSslPeersColumn.Name)),
		preferredIpPeersColumn.Name:            NewOptionalColumn(row.GetByColumn(preferredIpPeersColumn.Name)),
	}

	return NewHost(
		addr,
		port,
		*hostId,
		datacenter,
		rack,
		tokens,
		schemaId,
		columnData), nil
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

		log.Infof("Found host with 0.0.0.0 as rpc_address, using listen_address (%v) to contact it instead. "+
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
