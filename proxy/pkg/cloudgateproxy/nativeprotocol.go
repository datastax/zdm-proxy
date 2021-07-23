package cloudgateproxy

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type ParsedRow struct {
	ColumnIndexes map[string]int
	Columns       []*message.ColumnMetadata
	Values        []interface{}
}

func NewParsedRow(
	columnIndexes map[string]int,
	columns []*message.ColumnMetadata,
	values []interface{}) *ParsedRow {
	return &ParsedRow{
		ColumnIndexes: columnIndexes,
		Columns:       columns,
		Values:        values,
	}
}

type ParsedRowSet struct {
	ColumnIndexes map[string]int
	Columns       []*message.ColumnMetadata
	PagingState   []byte
	Rows          []*ParsedRow
}

func EncodeRowsResult(
	genericTypeCodec *GenericTypeCodec,
	columns []*message.ColumnMetadata,
	rows [][]interface{}) (*message.RowsResult, error) {
	encodedRows := make([]message.Row, len(rows))
	for rowIdx, parsedRow := range rows {
		newRow := make([]message.Column, len(columns))
		for colIdx, value := range parsedRow {
			encoded, err := genericTypeCodec.Encode(columns[colIdx].Type, value)
			if err != nil {
				return nil, fmt.Errorf("could not encode col %d of row %d: %w", colIdx, rowIdx, err)
			}
			newRow[colIdx] = encoded
		}
		encodedRows[rowIdx] = newRow
	}

	return &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(columns)),
			Columns:     columns,
		},
		Data: encodedRows,
	}, nil
}

func ParseRowsResult(
	genericTypeCodec *GenericTypeCodec,
	result *message.RowsResult,
	columns []*message.ColumnMetadata,
	columnsIndexes map[string]int) (*ParsedRowSet, error) {
	if columns == nil || columnsIndexes == nil {
		if result.Metadata.Columns == nil {
			if len(result.Data) == 0 {
				return &ParsedRowSet{
					ColumnIndexes: map[string]int{},
					Columns:       []*message.ColumnMetadata{},
					PagingState:   nil,
					Rows:          []*ParsedRow{},
				}, nil
			}

			return nil, fmt.Errorf(
				"could not parse rows result because the server did not return any column metadata")
		}
		columns = make([]*message.ColumnMetadata, len(result.Metadata.Columns))
		columnsIndexes = make(map[string]int)
		for idx, col := range result.Metadata.Columns {
			columns[idx] = col
			columnsIndexes[col.Name] = idx
		}
	}

	rows := make([]*ParsedRow, len(result.Data))

	for rowIdx, row := range result.Data {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("column metadata doesn't match row length")
		}

		newRow := make([]interface{}, len(row))
		for colIdx, value := range row {
			decoded, err := genericTypeCodec.Decode(columns[colIdx].Type, value)
			if err != nil {
				return nil, fmt.Errorf("could not parse col %d of row %d: %w", colIdx, rowIdx, err)
			}
			newRow[colIdx] = decoded
		}
		rows[rowIdx] = NewParsedRow(columnsIndexes, columns, newRow)
	}

	return &ParsedRowSet{
		ColumnIndexes: columnsIndexes,
		Columns:       columns,
		PagingState:   result.Metadata.PagingState,
		Rows:          rows,
	}, nil
}

func (recv *ParsedRow) GetByColumn(column string) (interface{}, bool) {
	colIdx, exists := recv.ColumnIndexes[column]
	if !exists {
		return nil, exists
	}

	return recv.Values[colIdx], exists
}

func (recv *ParsedRow) Get(idx int) (interface{}, error) {
	if idx >= len(recv.Values) {
		return nil, fmt.Errorf("index %d is >= than number of columns %d", idx, len(recv.Values))
	}

	return recv.Values[idx], nil
}

func (recv *ParsedRow) ContainsColumn(column string) bool {
	_, ok := recv.GetColumn(column)
	return ok
}

func (recv *ParsedRow) GetColumn(column string) (*message.ColumnMetadata, bool) {
	colIdx, ok := recv.ColumnIndexes[column]
	if !ok {
		return nil, false
	}

	return recv.Columns[colIdx], true
}

func (recv *ParsedRow) IsNull(column string) bool {
	val, ok := recv.GetByColumn(column)
	if !ok {
		return true
	}

	return val == nil
}

func buildDefaultTypeCodecMap() map[datatype.DataType]datatype.Codec {
	commonCodecs := map[datatype.DataType]datatype.Codec{
		datatype.Ascii:    &datatype.AsciiCodec{},
		datatype.Bigint:   &datatype.BigintCodec{},
		datatype.Blob:     &datatype.BlobCodec{},
		datatype.Boolean:  &datatype.BooleanCodec{},
		datatype.Counter:  &datatype.CounterCodec{},
		datatype.Decimal:  &datatype.DecimalCodec{},
		datatype.Double:   &datatype.DoubleCodec{},
		datatype.Float:    &datatype.FloatCodec{},
		datatype.Inet:     &datatype.InetCodec{},
		datatype.Int:      &datatype.IntCodec{},
		datatype.Smallint: &datatype.SmallintCodec{},
		datatype.Text:     &datatype.TextCodec{},
		datatype.Varchar:  &datatype.VarcharCodec{},
		datatype.Timeuuid: &datatype.TimeuuidCodec{},
		datatype.Tinyint:  &datatype.TinyintCodec{},
		datatype.Uuid:     &datatype.UuidCodec{},
		datatype.Varint:   &datatype.VarintCodec{},
	}
	return commonCodecs
}

func NewDefaultGenericTypeCodec(version primitive.ProtocolVersion) *GenericTypeCodec {
	return &GenericTypeCodec{
		version: version,
		codecs:  buildDefaultTypeCodecMap(),
	}
}

type GenericTypeCodec struct {
	version primitive.ProtocolVersion
	codecs  map[datatype.DataType]datatype.Codec
}

func (recv *GenericTypeCodec) Encode(dt datatype.DataType, val interface{}) (encoded []byte, err error) {
	codec, err := recv.createCodecFromDataType(dt)
	if err != nil {
		return nil, err
	}

	return codec.Encode(val, recv.version)
}

func (recv *GenericTypeCodec) Decode(dt datatype.DataType, encoded []byte) (decoded interface{}, err error) {
	codec, err := recv.createCodecFromDataType(dt)
	if err != nil {
		return nil, err
	}

	return codec.Decode(encoded, recv.version)
}

func (recv *GenericTypeCodec) createCodecFromDataType(dt datatype.DataType) (datatype.Codec, error) {
	switch dt.GetDataTypeCode() {
	case primitive.DataTypeCodeList:
		listType := dt.(datatype.ListType)
		elemCodec, err := recv.createCodecFromDataType(listType.GetElementType())
		if err != nil {
			return nil, err
		}
		return datatype.NewListCodec(elemCodec), nil
	case primitive.DataTypeCodeSet:
		setType := dt.(datatype.SetType)
		elemCodec, err := recv.createCodecFromDataType(setType.GetElementType())
		if err != nil {
			return nil, err
		}
		return datatype.NewSetCodec(elemCodec), nil
	case primitive.DataTypeCodeMap:
		mapType := dt.(datatype.MapType)
		keyCodec, err := recv.createCodecFromDataType(mapType.GetKeyType())
		if err != nil {
			return nil, err
		}
		valueCodec, err := recv.createCodecFromDataType(mapType.GetValueType())
		if err != nil {
			return nil, err
		}
		return datatype.NewMapCodec(keyCodec, valueCodec), nil
	case primitive.DataTypeCodeCustom, primitive.DataTypeCodeUdt:
		return &datatype.NilDecoderCodec{}, nil
	default:
		codec, ok := recv.codecs[dt]
		if !ok {
			return nil, fmt.Errorf("could not find codec for DataType %v", dt)
		}

		return codec, nil
	}
}

/*

cqlsh> describe system.local;
#3.11
CREATE TABLE system.local (
    key text PRIMARY KEY,
    bootstrapped text,
    broadcast_address inet,
    cluster_name text,
    cql_version text,
    data_center text,
    gossip_generation int,
    host_id uuid,
    listen_address inet,
    native_protocol_version text,
    partitioner text,
    rack text,
    release_version text,
    rpc_address inet,
    schema_version uuid,
    thrift_version text,
    tokens set<text>,
    truncated_at map<uuid, blob>
)
# DSE6.8
CREATE TABLE system.local (
    key text PRIMARY KEY,
    bootstrapped text,
    broadcast_address inet,
    cluster_name text,
    cql_version text,
    data_center text,
    dse_version text,
    gossip_generation int,
    graph boolean,
    host_id uuid,
    jmx_port int,
    last_nodesync_checkpoint_time bigint,
    listen_address inet,
    native_protocol_version text,
    native_transport_address inet,
    native_transport_port int,
    native_transport_port_ssl int,
    partitioner text,
    rack text,
    release_version text,
    rpc_address inet,
    schema_version uuid,
    server_id text,
    storage_port int,
    storage_port_ssl int,
    tokens set<text>,
    truncated_at map<uuid, blob>,
    workload text,
    workloads frozen<set<text>>
)
*/

var (
	keyColumn                        = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "key", Type: datatype.Varchar}
	bootstrappedColumn               = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "bootstrapped", Type: datatype.Varchar}
	broadcastAddressColumn           = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "broadcast_address", Type: datatype.Inet}
	clusterNameColumn                = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "cluster_name", Type: datatype.Varchar}
	cqlVersionColumn                 = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "cql_version", Type: datatype.Varchar}
	datacenterColumn                 = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "data_center", Type: datatype.Varchar}
	dseVersionColumn                 = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "dse_version", Type: datatype.Varchar}
	gossipGenerationColumn           = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "gossip_generation", Type: datatype.Int}
	graphColumn                      = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "graph", Type: datatype.Boolean}
	hostIdColumn                     = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "host_id", Type: datatype.Uuid}
	jmxPortColumn                    = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "jmx_port", Type: datatype.Int}
	lastNodesyncCheckpointTimeColumn = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "last_nodesync_checkpoint_time", Type: datatype.Bigint}
	listenAddressColumn              = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "listen_address", Type: datatype.Inet}
	nativeProtocolVersionColumn      = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "native_protocol_version", Type: datatype.Varchar}
	nativeTransportAddressColumn     = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "native_transport_address", Type: datatype.Inet}
	nativeTransportPortColumn        = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "native_transport_port", Type: datatype.Int}
	nativeTransportPortSslColumn     = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "native_transport_port_ssl", Type: datatype.Int}
	partitionerColumn                = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "partitioner", Type: datatype.Varchar}
	rackColumn                       = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "rack", Type: datatype.Varchar}
	releaseVersionColumn             = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "release_version", Type: datatype.Varchar}
	rpcAddressColumn                 = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "rpc_address", Type: datatype.Inet}
	schemaVersionColumn              = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "schema_version", Type: datatype.Uuid}
	serverIdColumn                   = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "server_id", Type: datatype.Varchar}
	storagePortColumn                = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "storage_port", Type: datatype.Int}
	storagePortSslColumn             = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "storage_port_ssl", Type: datatype.Int}
	thriftVersionColumn              = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "thrift_version", Type: datatype.Varchar}
	tokensColumn                     = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "tokens", Type: datatype.NewSetType(datatype.Varchar)}
	truncatedAtColumn                = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "truncated_at", Type: datatype.NewMapType(datatype.Uuid, datatype.Blob)}
	workloadColumn                   = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "workload", Type: datatype.Varchar}
	workloadsColumn                  = &message.ColumnMetadata{Keyspace: "system", Table: "local", Name: "workloads", Type: datatype.NewSetType(datatype.Varchar)}
)

var systemLocalColumns = []*message.ColumnMetadata{
	keyColumn,
	bootstrappedColumn,
	broadcastAddressColumn,
	clusterNameColumn,
	cqlVersionColumn,
	datacenterColumn,
	dseVersionColumn,
	gossipGenerationColumn,
	graphColumn,
	hostIdColumn,
	jmxPortColumn,
	lastNodesyncCheckpointTimeColumn,
	listenAddressColumn,
	nativeProtocolVersionColumn,
	nativeTransportPortColumn,
	nativeTransportPortSslColumn,
	partitionerColumn,
	nativeTransportAddressColumn,
	rackColumn,
	releaseVersionColumn,
	rpcAddressColumn,
	schemaVersionColumn,
	serverIdColumn,
	storagePortColumn,
	storagePortSslColumn,
	thriftVersionColumn,
	tokensColumn,
	truncatedAtColumn,
	workloadColumn,
	workloadsColumn,
}

type systemLocalInfo struct {
	key                        *optionalColumn
	clusterName                *optionalColumn
	bootstrapped               *optionalColumn
	cqlVersion                 *optionalColumn
	gossipGeneration           *optionalColumn
	lastNodesyncCheckpointTime *optionalColumn
	nativeProtocolVersion      *optionalColumn
	thriftVersion              *optionalColumn
	partitioner                *optionalColumn
}

type optionalColumn struct {
	column interface{}
	exists bool
}

func NewOptionalColumn(col interface{}, exists bool) *optionalColumn {
	return &optionalColumn{
		column: col,
		exists: exists,
	}
}

func (recv *optionalColumn) AsNillableString() *string {
	return recv.column.(*string)
}

func (recv *optionalColumn) AsNillableBigInt() *int64 {
	return recv.column.(*int64)
}

func (recv *optionalColumn) AsNillableInt() *int {
	return recv.column.(*int)
}

func NewSystemLocalRowsResult(
	genericTypeCodec *GenericTypeCodec, systemLocalInfo *systemLocalInfo, virtualHost *VirtualHost, proxyPort int) (*message.RowsResult, error) {

	columns := make([]*message.ColumnMetadata, 0, len(systemLocalColumns))
	row := make([]interface{}, 0, len(systemLocalColumns))
	addValueAndColumnIfExists(&row, &columns, keyColumn, systemLocalInfo.key)
	addValueAndColumnIfExists(&row, &columns, bootstrappedColumn, systemLocalInfo.bootstrapped)
	addValueAndColumn(&row, &columns, broadcastAddressColumn, virtualHost.Addr)
	addValueAndColumnIfExists(&row, &columns, clusterNameColumn, systemLocalInfo.clusterName)
	addValueAndColumnIfExists(&row, &columns, cqlVersionColumn, systemLocalInfo.cqlVersion)
	addValueAndColumn(&row, &columns, datacenterColumn, virtualHost.Host.Datacenter)
	addValueAndColumnIfExists(&row, &columns, dseVersionColumn, virtualHost.Host.DseVersion)
	addValueAndColumnIfExists(&row, &columns, gossipGenerationColumn, systemLocalInfo.gossipGeneration)
	addValueAndColumnIfExists(&row, &columns, graphColumn, virtualHost.Host.Graph)
	addValueAndColumn(&row, &columns, hostIdColumn, virtualHost.HostId)
	addValueAndColumnIfExists(&row, &columns, jmxPortColumn, virtualHost.Host.JmxPort)
	addValueAndColumnIfExists(&row, &columns, lastNodesyncCheckpointTimeColumn, systemLocalInfo.lastNodesyncCheckpointTime)
	addValueAndColumn(&row, &columns, listenAddressColumn, virtualHost.Addr)
	addValueAndColumnIfExists(&row, &columns, nativeProtocolVersionColumn, systemLocalInfo.nativeProtocolVersion)
	overrideValueAndColumnIfExists(&row, &columns, nativeTransportAddressColumn, virtualHost.Host.NativeTransportAddress, virtualHost.Addr)
	overrideValueAndColumnIfExists(&row, &columns, nativeTransportPortColumn, virtualHost.Host.NativeTransportPort, proxyPort)
	overrideValueAndColumnIfExists(&row, &columns, nativeTransportPortSslColumn, virtualHost.Host.NativeTransportPortSsl, proxyPort)
	addValueAndColumnIfExists(&row, &columns, partitionerColumn, systemLocalInfo.partitioner)
	addValueAndColumn(&row, &columns, rackColumn, "rack0")
	addValueAndColumn(&row, &columns, releaseVersionColumn, virtualHost.Host.ReleaseVersion)
	addValueAndColumn(&row, &columns, rpcAddressColumn, virtualHost.Addr)
	if virtualHost.Host.SchemaVersion == nil {
		addValueAndColumn(&row, &columns, schemaVersionColumn, nil)
	} else {
		schemaId, err := primitive.ParseUuid(virtualHost.Host.SchemaVersion.String())
		if err != nil {
			return nil, fmt.Errorf("could not encode schema version: %v", err)
		}
		addValueAndColumn(&row, &columns, schemaVersionColumn, schemaId)
	}
	addValueAndColumnIfExists(&row, &columns, serverIdColumn, virtualHost.Host.ServerId)
	addValueAndColumnIfExists(&row, &columns, storagePortColumn, virtualHost.Host.StoragePort)
	addValueAndColumnIfExists(&row, &columns, storagePortSslColumn, virtualHost.Host.StoragePortSsl)
	addValueAndColumnIfExists(&row, &columns, thriftVersionColumn, systemLocalInfo.thriftVersion)
	addValueAndColumn(&row, &columns, tokensColumn, virtualHost.Tokens)
	addValueAndColumn(&row, &columns, truncatedAtColumn, nil)
	addValueAndColumnIfExists(&row, &columns, workloadColumn, virtualHost.Host.Workload)
	addValueAndColumnIfExists(&row, &columns, workloadsColumn, virtualHost.Host.Workloads)

	return EncodeRowsResult(genericTypeCodec, columns, [][]interface{}{row})
}

func addValueAndColumnIfExists(row *[]interface{}, columns *[]*message.ColumnMetadata, columnMetadata *message.ColumnMetadata, col *optionalColumn) {
	if col.exists {
		*columns = append(*columns, columnMetadata)
		*row = append(*row, convertNullableToValue(col.column))
	}
}

func overrideValueAndColumnIfExists(row *[]interface{}, columns *[]*message.ColumnMetadata, columnMetadata *message.ColumnMetadata, col *optionalColumn, val interface{}) {
	if col.exists {
		*columns = append(*columns, columnMetadata)
		*row = append(*row, convertNullableToValue(val))
	}
}

func addValueAndColumn(row *[]interface{}, columns *[]*message.ColumnMetadata, columnMetadata *message.ColumnMetadata, val interface{}) {
	*row = append(*row, convertNullableToValue(val))
	*columns = append(*columns, columnMetadata)
}

/*

cqlsh> describe system.peers;
#3.11
CREATE TABLE system.peers (
    peer inet PRIMARY KEY,
    data_center text,
    host_id uuid,
    preferred_ip inet,
    rack text,
    release_version text,
    rpc_address inet,
    schema_version uuid,
    tokens set<text>
)

#DSE6.8
CREATE TABLE system.peers (
    peer inet PRIMARY KEY,
    data_center text,
    dse_version text,
    graph boolean,
    host_id uuid,
    jmx_port int,
    native_transport_address inet,
    native_transport_port int,
    native_transport_port_ssl int,
    preferred_ip inet,
    rack text,
    release_version text,
    rpc_address inet,
    schema_version uuid,
    server_id text,
    storage_port int,
    storage_port_ssl int,
    tokens set<text>,
    workload text,
    workloads frozen<set<text>>
)
*/

var (
	peerColumn                        = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "peer", Type: datatype.Inet}
	datacenterPeersColumn             = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "data_center", Type: datatype.Varchar}
	dseVersionPeersColumn             = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "dse_version", Type: datatype.Varchar}
	graphPeersColumn                  = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "graph", Type: datatype.Boolean}
	hostIdPeersColumn                 = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "host_id", Type: datatype.Uuid}
	jmxPortPeersColumn                = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "jmx_port", Type: datatype.Int}
	nativeTransportAddressPeersColumn = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "native_transport_address", Type: datatype.Inet}
	nativeTransportPortPeersColumn    = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "native_transport_port", Type: datatype.Int}
	nativeTransportPortSslPeersColumn = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "native_transport_port_ssl", Type: datatype.Int}
	preferredIpPeersColumn            = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "preferred_ip", Type: datatype.Inet}
	rackPeersColumn                   = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "rack", Type: datatype.Varchar}
	releaseVersionPeersColumn         = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "release_version", Type: datatype.Varchar}
	rpcAddressPeersColumn             = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "rpc_address", Type: datatype.Inet}
	schemaVersionPeersColumn          = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "schema_version", Type: datatype.Uuid}
	serverIdPeersColumn               = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "server_id", Type: datatype.Varchar}
	storagePortPeersColumn            = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "storage_port", Type: datatype.Int}
	storagePortSslPeersColumn         = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "storage_port_ssl", Type: datatype.Int}
	tokensPeersColumn                 = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "tokens", Type: datatype.NewSetType(datatype.Varchar)}
	workloadPeersColumn               = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "workload", Type: datatype.Varchar}
	workloadsPeersColumn              = &message.ColumnMetadata{Keyspace: "system", Table: "peers", Name: "workloads", Type: datatype.NewSetType(datatype.Varchar)}
)

var systemPeersColumns = []*message.ColumnMetadata{
	peerColumn,
	datacenterPeersColumn,
	dseVersionPeersColumn,
	graphPeersColumn,
	hostIdPeersColumn,
	jmxPortPeersColumn,
	nativeTransportAddressPeersColumn,
	nativeTransportPortPeersColumn,
	nativeTransportPortSslPeersColumn,
	preferredIpPeersColumn,
	rackPeersColumn,
	releaseVersionPeersColumn,
	rpcAddressPeersColumn,
	schemaVersionPeersColumn,
	serverIdPeersColumn,
	storagePortPeersColumn,
	storagePortSslPeersColumn,
	tokensPeersColumn,
	workloadPeersColumn,
	workloadsPeersColumn,
}

func NewSystemPeersRowsResult(
	genericTypeCodec *GenericTypeCodec, virtualHosts []*VirtualHost, localVirtualHostIndex int, proxyPort int, preferredIpColExists bool) (*message.RowsResult, error) {

	columns := make([]*message.ColumnMetadata, 0, len(systemPeersColumns))
	addedColumns := make(map[string]interface{})
	rows := make([][]interface{}, 0, len(virtualHosts) - 1)
	first := true
	for i := 0; i < len(virtualHosts); i++ {
		errors := make([]error, 0)

		if i == localVirtualHostIndex && i != 0 && len(virtualHosts) != 1 {
			continue
		}

		virtualHost := virtualHosts[i]

		proxyAddress := virtualHost.Addr
		host := virtualHost.Host
		tokens := virtualHost.Tokens

		row := make([]interface{}, 0, len(columns))

		addPeerColumn(first, &row, &columns, addedColumns, peerColumn, proxyAddress, &errors)
		addPeerColumn(first, &row, &columns, addedColumns, datacenterPeersColumn, host.Datacenter, &errors)
		addPeerColumnIfExists(first, &row, &columns, addedColumns, dseVersionPeersColumn, host.DseVersion, &errors)
		addPeerColumnIfExists(first, &row, &columns, addedColumns, graphPeersColumn, host.Graph, &errors)
		addPeerColumn(first, &row, &columns, addedColumns, hostIdPeersColumn, virtualHost.HostId, &errors)
		addPeerColumnIfExists(first, &row, &columns, addedColumns, jmxPortPeersColumn, host.JmxPort, &errors)
		overridePeerColumnIfExists(first, &row, &columns, addedColumns, nativeTransportAddressPeersColumn, host.NativeTransportAddress, proxyAddress, &errors)
		overridePeerColumnIfExists(first, &row, &columns, addedColumns, nativeTransportPortPeersColumn, host.NativeTransportPort, proxyPort, &errors)
		overridePeerColumnIfExists(first, &row, &columns, addedColumns, nativeTransportPortSslPeersColumn, host.NativeTransportPortSsl, proxyPort, &errors)
		if preferredIpColExists {
			addPeerColumn(first, &row, &columns, addedColumns, preferredIpPeersColumn, proxyAddress, &errors)
		}
		addPeerColumn(first, &row, &columns, addedColumns, rackPeersColumn, "rack0", &errors)
		addPeerColumn(first, &row, &columns, addedColumns, releaseVersionPeersColumn, host.ReleaseVersion, &errors)
		addPeerColumn(first, &row, &columns, addedColumns, rpcAddressPeersColumn, proxyAddress, &errors)
		if host.SchemaVersion == nil {
			addPeerColumn(first, &row, &columns, addedColumns, schemaVersionPeersColumn, nil, &errors)
		} else {
			schemaId, err := primitive.ParseUuid(host.SchemaVersion.String())
			if err != nil {
				return nil, fmt.Errorf("could not encode schema version: %v", err)
			}
			addPeerColumn(first, &row, &columns, addedColumns, schemaVersionPeersColumn, schemaId, &errors)
		}
		addPeerColumnIfExists(first, &row, &columns, addedColumns, serverIdPeersColumn, host.ServerId, &errors)
		addPeerColumnIfExists(first, &row, &columns, addedColumns, storagePortPeersColumn, host.StoragePort, &errors)
		addPeerColumnIfExists(first, &row, &columns, addedColumns, storagePortSslPeersColumn, host.StoragePortSsl, &errors)
		addPeerColumn(first, &row, &columns, addedColumns, tokensPeersColumn, tokens, &errors)
		addPeerColumnIfExists(first, &row, &columns, addedColumns, workloadPeersColumn, host.Workload, &errors)
		addPeerColumnIfExists(first, &row, &columns, addedColumns, workloadsPeersColumn, host.Workloads, &errors)
		if len(errors) > 0 {
			return nil, fmt.Errorf("errors adding peer columns for host %v: %v", host.Address.String(), errors)
		}
		rows = append(rows, row)
		first = false
	}

	if localVirtualHostIndex == 0 && len(virtualHosts) == 1 {
		rows = [][]interface{}{}
	}

	return EncodeRowsResult(genericTypeCodec, columns, rows)
}

func addPeerColumnHelper(first bool, row *[]interface{}, columns *[]*message.ColumnMetadata, addedColumnsByName map[string]interface{}, columnMetadata *message.ColumnMetadata, colExists bool, val interface{}, errors *[]error) {
	if first {
		if colExists {
			*columns = append(*columns, columnMetadata)
			*row = append(*row, convertNullableToValue(val))
			addedColumnsByName[columnMetadata.Name] = true
		}
	} else {
		_, columnIsAdded := addedColumnsByName[columnMetadata.Name]
		if columnIsAdded != colExists {
			*errors = append(*errors, fmt.Errorf("columns mismatch between hosts: " +
				"columnIsAdded=%v existsInHost=%v name=%v", columnIsAdded, colExists, columnMetadata.Name))
			return
		}

		if colExists {
			*row = append(*row, convertNullableToValue(val))
		}
	}
}

func addPeerColumnIfExists(first bool, row *[]interface{}, columns *[]*message.ColumnMetadata,
	addedColumnsByName map[string]interface{}, columnMetadata *message.ColumnMetadata, col *optionalColumn, errors *[]error) {
	addPeerColumnHelper(first, row, columns, addedColumnsByName, columnMetadata, col.exists, col.column, errors)
}

func overridePeerColumnIfExists(first bool, row *[]interface{}, columns *[]*message.ColumnMetadata,
	addedColumnsByName map[string]interface{}, columnMetadata *message.ColumnMetadata, col *optionalColumn, val interface{}, errors *[]error) {
	addPeerColumnHelper(first, row, columns, addedColumnsByName, columnMetadata, col.exists, val, errors)
}

func addPeerColumn(first bool, row *[]interface{}, columns *[]*message.ColumnMetadata,
	addedColumnsByName map[string]interface{}, columnMetadata *message.ColumnMetadata, val interface{}, errors *[]error) {
	addPeerColumnHelper(first, row, columns, addedColumnsByName, columnMetadata, true, val, errors)
}

func convertNullableToValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	switch typedVal := val.(type) {
	case *string:
		if typedVal == nil {
			return nil
		}
		return *typedVal
	case *int:
		if typedVal == nil {
			return nil
		}
		return *typedVal
	case *int32:
		if typedVal == nil {
			return nil
		}
		return *typedVal
	case *int64:
		if typedVal == nil {
			return nil
		}
		return *typedVal
	case *bool:
		if typedVal == nil {
			return nil
		}
		return *typedVal
	default:
		return val
	}
}
