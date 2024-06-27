package integration_tests

import (
	"bytes"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"net"
	"sort"
	"strings"
)

const murmur3Partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
const randomPartitioner = "org.apache.cassandra.dht.RandomPartitioner"
const byteOrderedPartitioner = "org.apache.cassandra.dht.ByteOrderedPartitioner"

// NewCustomSystemTablesHandler creates a new RequestHandler to handle queries to system tables (system.local and system.peers).
// If no custom partitioner is specified, it will default to Murmur3
func NewCustomSystemTablesHandler(cluster string, datacenter string, peersIpPrefix string, peersCount map[string]int, customPartitioner string) client.RequestHandler {
	return func(request *frame.Frame, conn *client.CqlServerConnection, _ client.RequestHandlerContext) (response *frame.Frame) {
		if query, ok := request.Body.Message.(*message.Query); ok {
			q := strings.TrimSpace(strings.ToLower(query.Query))
			q = strings.Join(strings.Fields(q), " ") // remove extra whitespace
			if strings.HasPrefix(q, "select * from system.local") {
				log.Debugf("%v: [system tables handler]: returning full system.local", conn)
				response = fullSystemLocal(cluster, datacenter, customPartitioner, request, conn)
			} else if strings.HasPrefix(q, "select schema_version from system.local") {
				log.Debugf("%v: [system tables handler]: returning schema_version", conn)
				response = schemaVersion(request)
			} else if strings.HasPrefix(q, "select cluster_name from system.local") {
				log.Debugf("%v: [system tables handler]: returning cluster_name", conn)
				response = clusterName(cluster, request)
			} else if strings.Contains(q, "from system.peers") {
				log.Debugf("%v: [system tables handler]: returning empty system.peers", conn)
				response = fullSystemPeers(request, conn, peersIpPrefix, peersCount)
			}
		}
		return
	}
}

var (
	keyColumn              = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "key", Type: datatype.Varchar}
	broadcastAddressColumn = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "broadcast_address", Type: datatype.Inet}
	clusterNameColumn      = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "cluster_name", Type: datatype.Varchar}
	cqlVersionColumn       = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "cql_version", Type: datatype.Varchar}
	datacenterColumn       = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "data_center", Type: datatype.Varchar}
	hostIdColumn           = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "host_id", Type: datatype.Uuid}
	listenAddressColumn    = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "listen_address", Type: datatype.Inet}
	partitionerColumn      = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "partitioner", Type: datatype.Varchar}
	rackColumn             = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "rack", Type: datatype.Varchar}
	releaseVersionColumn   = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "release_version", Type: datatype.Varchar}
	rpcAddressColumn       = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "rpc_address", Type: datatype.Inet}
	schemaVersionColumn    = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "schema_version", Type: datatype.Uuid}
	tokensColumn           = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "tokens", Type: datatype.NewSet(datatype.Varchar)}
)

// These columns are a subset of the total columns returned by OSS C* 3.11.2, and contain all the information that
// drivers need in order to establish the cluster topology and determine its characteristics.
var systemLocalColumns = []*message.ColumnMetadata{
	keyColumn,
	broadcastAddressColumn,
	clusterNameColumn,
	cqlVersionColumn,
	datacenterColumn,
	hostIdColumn,
	listenAddressColumn,
	partitionerColumn,
	rackColumn,
	releaseVersionColumn,
	rpcAddressColumn,
	schemaVersionColumn,
	tokensColumn,
}

var systemLocalColumnsV2 = []*message.ColumnMetadata{
	keyColumn,
	clusterNameColumn,
	cqlVersionColumn,
	datacenterColumn,
	hostIdColumn,
	partitionerColumn,
	rackColumn,
	releaseVersionColumn,
	schemaVersionColumn,
	tokensColumn,
}

var (
	peerColumn                = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "peer", Type: datatype.Inet}
	datacenterPeersColumn     = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "data_center", Type: datatype.Varchar}
	hostIdPeersColumn         = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "host_id", Type: datatype.Uuid}
	rackPeersColumn           = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "rack", Type: datatype.Varchar}
	releaseVersionPeersColumn = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "release_version", Type: datatype.Varchar}
	rpcAddressPeersColumn     = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "rpc_address", Type: datatype.Inet}
	schemaVersionPeersColumn  = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "schema_version", Type: datatype.Uuid}
	tokensPeersColumn         = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "tokens", Type: datatype.NewSet(datatype.Varchar)}
)

// These columns are a subset of the total columns returned by OSS C* 3.11.2, and contain all the information that
// drivers need in order to establish the cluster topology and determine its characteristics.
var systemPeersColumns = []*message.ColumnMetadata{
	peerColumn,
	datacenterColumn,
	hostIdColumn,
	rackColumn,
	releaseVersionColumn,
	rpcAddressColumn,
	schemaVersionColumn,
	tokensColumn,
}

var (
	keyValue                = message.Column("local")
	cqlVersionValue         = message.Column("3.4.4")
	hostIdValue             = message.Column{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
	defaultPartitionerValue = message.Column(murmur3Partitioner)
	rackValue               = message.Column("rack1")
	releaseVersionValue     = message.Column("3.11.2")
	schemaVersionValue      = message.Column{0xC0, 0xD1, 0xD2, 0x1E, 0xBB, 0x01, 0x41, 0x96, 0x86, 0xDB, 0xBC, 0x31, 0x7B, 0xC1, 0x79, 0x6A}
)

func systemLocalRow(cluster string, datacenter string, customPartitioner string, addr *net.Addr, version primitive.ProtocolVersion) message.Row {
	addrBuf := &bytes.Buffer{}
	if addr != nil {
		inetAddr := (*addr).(*net.TCPAddr).IP
		if inetAddr.To4() != nil {
			addrBuf.Write(inetAddr.To4())
		} else {
			addrBuf.Write(inetAddr)
		}
	}
	// emulate {'-9223372036854775808'} (entire ring)
	tokensBuf := &bytes.Buffer{}
	if version >= primitive.ProtocolVersion3 {
		_ = primitive.WriteInt(1, tokensBuf)
		_ = primitive.WriteInt(int32(len("-9223372036854775808")), tokensBuf)
	} else {
		_ = primitive.WriteShort(1, tokensBuf)
		_ = primitive.WriteShort(uint16(len("-9223372036854775808")), tokensBuf)
	}
	tokensBuf.WriteString("-9223372036854775808")

	partitionerValue := defaultPartitionerValue
	if customPartitioner != "" {
		partitionerValue = message.Column(customPartitioner)
	}
	if addrBuf.Len() > 0 {
		return message.Row{
			keyValue,
			addrBuf.Bytes(),
			message.Column(cluster),
			cqlVersionValue,
			message.Column(datacenter),
			hostIdValue,
			addrBuf.Bytes(),
			partitionerValue,
			rackValue,
			releaseVersionValue,
			addrBuf.Bytes(),
			schemaVersionValue,
			tokensBuf.Bytes(),
		}
	}
	return message.Row{
		keyValue,
		message.Column(cluster),
		cqlVersionValue,
		message.Column(datacenter),
		hostIdValue,
		partitionerValue,
		rackValue,
		releaseVersionValue,
		schemaVersionValue,
		tokensBuf.Bytes(),
	}
}

func fullSystemLocal(cluster string, datacenter string, customPartitioner string, request *frame.Frame, conn *client.CqlServerConnection) *frame.Frame {
	localAddress := conn.LocalAddr()
	systemLocalRow := systemLocalRow(cluster, datacenter, customPartitioner, &localAddress, request.Header.Version)
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(systemLocalColumns)),
			Columns:     systemLocalColumns,
		},
		Data: message.RowSet{systemLocalRow},
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}

func systemPeersRow(datacenter string, addr net.Addr, version primitive.ProtocolVersion) message.Row {
	addrBuf := &bytes.Buffer{}
	inetAddr := addr.(*net.TCPAddr).IP
	if inetAddr.To4() != nil {
		addrBuf.Write(inetAddr.To4())
	} else {
		addrBuf.Write(inetAddr)
	}

	uuidBuf := &bytes.Buffer{}
	hostId, _ := primitive.ParseUuid(uuid.New().String())
	_ = primitive.WriteUuid(hostId, uuidBuf)

	// emulate {'-9223372036854775808'} (entire ring)
	tokensBuf := &bytes.Buffer{}
	if version >= primitive.ProtocolVersion3 {
		_ = primitive.WriteInt(1, tokensBuf)
		_ = primitive.WriteInt(int32(len("-9223372036854775808")), tokensBuf)
	} else {
		_ = primitive.WriteShort(1, tokensBuf)
		_ = primitive.WriteShort(uint16(len("-9223372036854775808")), tokensBuf)
	}
	tokensBuf.WriteString("-9223372036854775808")
	return message.Row{
		addrBuf.Bytes(),
		message.Column(datacenter),
		uuidBuf.Bytes(),
		rackValue,
		releaseVersionValue,
		addrBuf.Bytes(),
		schemaVersionValue,
		tokensBuf.Bytes(),
	}
}

func fullSystemPeers(
	request *frame.Frame, localConn *client.CqlServerConnection, ipPrefix string, peersCount map[string]int) *frame.Frame {
	systemLocalRows := message.RowSet{}
	localAddr := localConn.LocalAddr().(*net.TCPAddr)
	totalCount := 0
	peersKeys := make([]string, 0)
	for key := range peersCount {
		peersKeys = append(peersKeys, key)
	}
	sort.Strings(peersKeys)
	for _, dc := range peersKeys {
		count := peersCount[dc]
		for i := 0; i < count; i++ {
			newRow := systemPeersRow(
				dc,
				&net.TCPAddr{
					IP:   net.ParseIP(fmt.Sprintf("%s%d", ipPrefix, totalCount+i+1)),
					Port: localAddr.Port,
					Zone: localAddr.Zone,
				},
				request.Header.Version)
			systemLocalRows = append(systemLocalRows, newRow)
		}
		totalCount += count
	}
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(systemPeersColumns)),
			Columns:     systemPeersColumns,
		},
		Data: systemLocalRows,
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}

func schemaVersion(request *frame.Frame) *frame.Frame {
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: 1,
			Columns:     []*message.ColumnMetadata{schemaVersionColumn},
		},
		Data: message.RowSet{message.Row{schemaVersionValue}},
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}

func clusterName(cluster string, request *frame.Frame) *frame.Frame {
	msg := &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: 1,
			Columns:     []*message.ColumnMetadata{clusterNameColumn},
		},
		Data: message.RowSet{message.Row{message.Column(cluster)}},
	}
	return frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
}
