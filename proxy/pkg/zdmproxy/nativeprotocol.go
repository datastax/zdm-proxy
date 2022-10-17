package zdmproxy

import (
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"strings"
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
	version primitive.ProtocolVersion,
	columns []*message.ColumnMetadata,
	rows [][]interface{}) (*message.RowsResult, error) {
	encodedRows := make([]message.Row, len(rows))
	for rowIdx, parsedRow := range rows {
		newRow := make([]message.Column, len(columns))
		for colIdx, value := range parsedRow {
			encoded, err := genericTypeCodec.Encode(columns[colIdx].Type, value, version)
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
	version primitive.ProtocolVersion,
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
			decoded, err := genericTypeCodec.Decode(columns[colIdx].Type, value, version)
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

func EncodePreparedResult(
	prepareRequestInfo *PrepareRequestInfo, connectionKeyspace string, columns []*message.ColumnMetadata) (
	*message.PreparedResult, error) {
	if len(columns) == 0 {
		return nil, errors.New("could not compute column metadata for system peers prepared result")
	}

	query := prepareRequestInfo.GetQuery()
	keyspace := prepareRequestInfo.GetKeyspace()
	if keyspace == "" {
		keyspace = connectionKeyspace
	}
	id := md5.Sum([]byte(query + keyspace))
	return &message.PreparedResult{
		PreparedQueryId: id[:],
		ResultMetadata: &message.RowsMetadata{
			ColumnCount: int32(len(columns)),
			Columns:     columns,
		},
	}, nil
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
	keyColumn                        = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "key", Type: datatype.Varchar}
	bootstrappedColumn               = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "bootstrapped", Type: datatype.Varchar}
	broadcastAddressColumn           = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "broadcast_address", Type: datatype.Inet}
	clusterNameColumn                = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "cluster_name", Type: datatype.Varchar}
	cqlVersionColumn                 = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "cql_version", Type: datatype.Varchar}
	datacenterColumn                 = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "data_center", Type: datatype.Varchar}
	dseVersionColumn                 = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "dse_version", Type: datatype.Varchar}
	gossipGenerationColumn           = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "gossip_generation", Type: datatype.Int}
	graphColumn                      = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "graph", Type: datatype.Boolean}
	hostIdColumn                     = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "host_id", Type: datatype.Uuid}
	jmxPortColumn                    = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "jmx_port", Type: datatype.Int}
	lastNodesyncCheckpointTimeColumn = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "last_nodesync_checkpoint_time", Type: datatype.Bigint}
	listenAddressColumn              = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "listen_address", Type: datatype.Inet}
	nativeProtocolVersionColumn      = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "native_protocol_version", Type: datatype.Varchar}
	nativeTransportAddressColumn     = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "native_transport_address", Type: datatype.Inet}
	nativeTransportPortColumn        = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "native_transport_port", Type: datatype.Int}
	nativeTransportPortSslColumn     = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "native_transport_port_ssl", Type: datatype.Int}
	partitionerColumn                = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "partitioner", Type: datatype.Varchar}
	rackColumn                       = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "rack", Type: datatype.Varchar}
	releaseVersionColumn             = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "release_version", Type: datatype.Varchar}
	rpcAddressColumn                 = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "rpc_address", Type: datatype.Inet}
	schemaVersionColumn              = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "schema_version", Type: datatype.Uuid}
	serverIdColumn                   = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "server_id", Type: datatype.Varchar}
	storagePortColumn                = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "storage_port", Type: datatype.Int}
	storagePortSslColumn             = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "storage_port_ssl", Type: datatype.Int}
	thriftVersionColumn              = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "thrift_version", Type: datatype.Varchar}
	tokensColumn                     = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "tokens", Type: datatype.NewSetType(datatype.Varchar)}
	truncatedAtColumn                = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "truncated_at", Type: datatype.NewMapType(datatype.Uuid, datatype.Blob)}
	workloadColumn                   = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "workload", Type: datatype.Varchar}
	workloadsColumn                  = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemLocalTableName, Name: "workloads", Type: datatype.NewSetType(datatype.Varchar)}
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

type ColumnNotFoundErr struct {
	Name string
}

func (recv *ColumnNotFoundErr) Error() string {
	return fmt.Sprintf("could not find column: %v", recv.Name)
}

func findColumnMetadata(cols []*message.ColumnMetadata, name string) *message.ColumnMetadata {
	for _, col := range cols {
		if col.Name == name {
			return col
		}
	}
	return nil
}

func columnFromSelector(
	cols []*message.ColumnMetadata, parsedSelector selector,
	keyspace string, table string) (resultColumn *message.ColumnMetadata, isCountSelector bool, err error) {
	switch s := parsedSelector.(type) {
	case *countSelector:
		return &message.ColumnMetadata{
			Keyspace: keyspace,
			Table:    table,
			Name:     s.name,
			Type:     datatype.Int,
		}, true, nil
	case *idSelector:
		if column := findColumnMetadata(cols, s.name); column != nil {
			return column, false, nil
		} else {
			return nil, false, &ColumnNotFoundErr{Name: s.name}
		}
	case *aliasedSelector:
		// aliasedSelector contains the unaliasedSelector + an alias, we need to retrieve the unaliasedSelector
		resultColumn, isCountSelector, err = columnFromSelector(cols, s.selector, keyspace, table)
		if err != nil {
			return nil, false, err
		}

		// we are assuming here that resultColumn always refers to an unaliased column because the cql grammar doesn't support alias recursion
		aliasedColumn := resultColumn.Clone()
		aliasedColumn.Name = s.alias
		return aliasedColumn, isCountSelector, nil
	default:
		return nil, false, errors.New("unhandled selector type")
	}
}

func unaliasedColumnNameFromSelector(parsedSelector selector) (string, error) {
	switch s := parsedSelector.(type) {
	case *countSelector:
		return s.name, nil
	case *idSelector:
		return s.name, nil
	case *aliasedSelector:
		return s.selector.Name(), nil
	default:
		return "", fmt.Errorf("unhandled selector type: %T", s)
	}
}

func addSystemColumnValue(
	isStarSelector bool, first bool, row *[]interface{}, columns *[]*message.ColumnMetadata, columnIndex int,
	col *message.ColumnMetadata, unaliasedColumnName string, peersColumns map[string]bool,
	systemLocalColumnData map[string]*optionalColumn, virtualHost *VirtualHost,
	proxyPort int, rowCount int) error {
	switch unaliasedColumnName {
	case peerColumn.Name, broadcastAddressColumn.Name, listenAddressColumn.Name, rpcAddressColumn.Name, preferredIpPeersColumn.Name:
		return addColumn(isStarSelector, first, row, columns, col, virtualHost.Addr)
	case datacenterColumn.Name:
		return addColumn(isStarSelector, first, row, columns, col, virtualHost.Host.Datacenter)
	case hostIdColumn.Name:
		return addColumn(isStarSelector, first, row, columns, col, virtualHost.HostId)
	case rackColumn.Name:
		return addColumn(isStarSelector, first, row, columns, col, virtualHost.Rack)
	case tokensColumn.Name:
		return addColumn(isStarSelector, first, row, columns, col, virtualHost.Tokens)
	case truncatedAtColumn.Name:
		return addColumn(isStarSelector, first, row, columns, col, nil)
	case partitionerColumn.Name:
		return addColumn(isStarSelector, first, row, columns, col, virtualHost.Partitioner)
	case schemaVersionColumn.Name:
		if virtualHost.Host.SchemaVersion == nil {
			return addColumn(isStarSelector, first, row, columns, col, nil)
		} else {
			schemaId := primitive.UUID(*virtualHost.Host.SchemaVersion)
			return addColumn(isStarSelector, first, row, columns, col, &schemaId)
		}
	}

	if strings.ToLower(unaliasedColumnName) == "count" {
		return addColumn(isStarSelector, first, row, columns, col, rowCount)
	}

	optionalCol, ok := virtualHost.Host.ColumnData[unaliasedColumnName]
	if ok {
		if peersColumns != nil {
			_, peersColumnExists := peersColumns[unaliasedColumnName]
			if !peersColumnExists {
				if isStarSelector {
					return nil
				} else {
					return fmt.Errorf("no peer column value for %s", unaliasedColumnName)
				}
			}
		}

		switch unaliasedColumnName {
		case nativeTransportAddressColumn.Name:
			return overrideColumnIfExists(isStarSelector, first, row, columns, col, optionalCol, virtualHost.Addr)
		case nativeTransportPortColumn.Name:
			return overrideColumnIfExists(isStarSelector, first, row, columns, col, optionalCol, proxyPort)
		case nativeTransportPortSslColumn.Name:
			return overrideColumnIfExists(isStarSelector, first, row, columns, col, optionalCol, proxyPort)
		default:
			return addColumnIfExists(isStarSelector, first, row, columns, col, optionalCol)
		}
	}

	if peersColumns == nil {
		if optionalCol, ok = systemLocalColumnData[unaliasedColumnName]; ok {
			return addColumnIfExists(isStarSelector, first, row, columns, col, optionalCol)
		}
	}

	return fmt.Errorf("no column value for %s", unaliasedColumnName)
}

func filterSystemColumns(
	parsedSelectClause *selectClause, systemTableColumns []*message.ColumnMetadata,
	tableName string) (resultCols []*message.ColumnMetadata, hasCountSelector bool, err error) {
	clonedSystemColumns := make([]*message.ColumnMetadata, len(systemTableColumns))
	copy(clonedSystemColumns, systemTableColumns)

	if parsedSelectClause.IsStarSelectClause() {
		resultCols = clonedSystemColumns
		hasCountSelector = false
	} else {
		selectors := parsedSelectClause.GetSelectors()
		resultCols = make([]*message.ColumnMetadata, 0, len(selectors))
		for _, parsedSelector := range selectors {
			col, isCountSelector, err := columnFromSelector(clonedSystemColumns, parsedSelector, systemKeyspaceName, tableName)
			if err != nil {
				return nil, false, err
			}
			hasCountSelector = hasCountSelector || isCountSelector
			resultCols = append(resultCols, col)
		}
	}

	return resultCols, hasCountSelector, nil
}

func getFilteredSystemValues(
	table string, parsedSelectClause *selectClause, first bool, columns *[]*message.ColumnMetadata,
	resultColumns []*message.ColumnMetadata, peerColumnNames map[string]bool,
	systemLocalColumnData map[string]*optionalColumn, virtualHost *VirtualHost,
	proxyPort int, rowCount int) ([]interface{}, error) {

	row := make([]interface{}, 0, len(resultColumns))
	if parsedSelectClause.IsStarSelectClause() {
		for i, col := range resultColumns {
			err := addSystemColumnValue(
				true, first, &row, columns, i, col, col.Name, peerColumnNames,
				systemLocalColumnData, virtualHost, proxyPort, rowCount)

			if err != nil {
				return nil, fmt.Errorf("errors adding columns for system %v result: %w", table, err)
			}
		}
	} else {
		if len(parsedSelectClause.selectors) != len(resultColumns) {
			return nil, fmt.Errorf("mismatch between number of selectors and result columns; selectors=%v, resultColumns=%v",
				len(parsedSelectClause.selectors), len(resultColumns))
		}
		for i, col := range parsedSelectClause.selectors {
			unaliasedColumnName, err := unaliasedColumnNameFromSelector(col)
			if err == nil {
				err = addSystemColumnValue(
					false, first, &row, columns, i, resultColumns[i], unaliasedColumnName,
					peerColumnNames, systemLocalColumnData, virtualHost, proxyPort, rowCount)
			}

			if err != nil {
				return nil, fmt.Errorf("errors adding columns for system %v result: %w", table, err)
			}
		}
	}
	return row, nil
}

// NewSystemLocalResult returns a PreparedResult if the prepareRequestInfo parameter is not nil and it returns a
// RowsResult if prepareRequestInfo is nil.
func NewSystemLocalResult(
	prepareRequestInfo *PrepareRequestInfo, connectionKeyspace string, genericTypeCodec *GenericTypeCodec,
	version primitive.ProtocolVersion, systemLocalColumnData map[string]*optionalColumn,
	parsedSelectClause *selectClause, virtualHost *VirtualHost, proxyPort int) (message.Result, error) {

	resultCols, _, err := filterSystemColumns(parsedSelectClause, systemLocalColumns, systemLocalTableName)
	if err != nil {
		return nil, err
	}
	rowCount := 1

	columns := make([]*message.ColumnMetadata, 0, len(resultCols))
	row, err := getFilteredSystemValues(
		"local", parsedSelectClause, true, &columns, resultCols,
		nil, systemLocalColumnData, virtualHost, proxyPort, rowCount)
	if err != nil {
		return nil, fmt.Errorf("errors adding columns for system local result (prepare=%v): %w", prepareRequestInfo != nil, err)
	}

	if prepareRequestInfo != nil {
		return EncodePreparedResult(prepareRequestInfo, connectionKeyspace, columns)
	} else {
		return EncodeRowsResult(genericTypeCodec, version, columns, [][]interface{}{row})
	}
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
	peerColumn                        = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "peer", Type: datatype.Inet}
	datacenterPeersColumn             = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "data_center", Type: datatype.Varchar}
	dseVersionPeersColumn             = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "dse_version", Type: datatype.Varchar}
	graphPeersColumn                  = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "graph", Type: datatype.Boolean}
	hostIdPeersColumn                 = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "host_id", Type: datatype.Uuid}
	jmxPortPeersColumn                = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "jmx_port", Type: datatype.Int}
	nativeTransportAddressPeersColumn = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "native_transport_address", Type: datatype.Inet}
	nativeTransportPortPeersColumn    = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "native_transport_port", Type: datatype.Int}
	nativeTransportPortSslPeersColumn = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "native_transport_port_ssl", Type: datatype.Int}
	preferredIpPeersColumn            = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "preferred_ip", Type: datatype.Inet}
	rackPeersColumn                   = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "rack", Type: datatype.Varchar}
	releaseVersionPeersColumn         = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "release_version", Type: datatype.Varchar}
	rpcAddressPeersColumn             = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "rpc_address", Type: datatype.Inet}
	schemaVersionPeersColumn          = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "schema_version", Type: datatype.Uuid}
	serverIdPeersColumn               = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "server_id", Type: datatype.Varchar}
	storagePortPeersColumn            = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "storage_port", Type: datatype.Int}
	storagePortSslPeersColumn         = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "storage_port_ssl", Type: datatype.Int}
	tokensPeersColumn                 = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "tokens", Type: datatype.NewSetType(datatype.Varchar)}
	workloadPeersColumn               = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "workload", Type: datatype.Varchar}
	workloadsPeersColumn              = &message.ColumnMetadata{Keyspace: systemKeyspaceName, Table: systemPeersTableName, Name: "workloads", Type: datatype.NewSetType(datatype.Varchar)}
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

// NewSystemPeersResult returns a PreparedResult if the prepareRequestInfo parameter is not nil and it returns a
// RowsResult if prepareRequestInfo is nil.
func NewSystemPeersResult(
	prepareRequestInfo *PrepareRequestInfo, connectionKeyspace string, genericTypeCodec *GenericTypeCodec,
	version primitive.ProtocolVersion, peerColumnNames map[string]bool, systemLocalColumnData map[string]*optionalColumn,
	parsedSelectClause *selectClause, virtualHosts []*VirtualHost, localVirtualHostIndex int, proxyPort int) (message.Result, error) {

	resultColumns, hasCountSelector, err := filterSystemColumns(parsedSelectClause, systemPeersColumns, systemPeersTableName)
	if err != nil {
		return nil, err
	}
	columns := make([]*message.ColumnMetadata, 0, len(resultColumns))
	rows := make([][]interface{}, 0, len(virtualHosts)-1)
	isFirstRow := true

	// at least 1 iteration of this for cycle should be executed even if there is no peers rows to be returned
	// so that the column metadata slice is filled
	for i := 0; i < len(virtualHosts); i++ {

		// skip this iteration if the current index matches the local proxy instance (so that it doesn't add itself to the peers table)
		// but don't skip if this proxy instance is the only one (there are no peers) so that the columns are added
		// we delete the row data afterwards if this resulted in the proxy adding itself to the peers row result
		if i == localVirtualHostIndex && len(virtualHosts) != 1 {
			continue
		}

		virtualHost := virtualHosts[i]

		row, err := getFilteredSystemValues(
			systemPeersTableName, parsedSelectClause, isFirstRow, &columns, resultColumns,
			peerColumnNames, systemLocalColumnData, virtualHost, proxyPort, len(virtualHosts)-1)
		if err != nil {
			return nil, fmt.Errorf("errors adding columns for system peers result: %w", err)
		}

		if prepareRequestInfo != nil {
			// we only need column metadata and only 1 iteration needed to compute that
			break
		}

		rows = append(rows, row)
		if isFirstRow {
			isFirstRow = false
			resultColumns = columns // final column list is set (relevant for star selector where result columns are not static)
		}
		if hasCountSelector {
			break
		}
	}

	if prepareRequestInfo != nil {
		return EncodePreparedResult(prepareRequestInfo, connectionKeyspace, columns)
	}

	// delete rows if the proxy added itself to the peers rows result
	if localVirtualHostIndex == 0 && len(virtualHosts) == 1 && !hasCountSelector {
		rows = [][]interface{}{}
	} else if hasCountSelector && len(virtualHosts) == 1 {
		for i, parsedSelector := range parsedSelectClause.GetSelectors() {
			isCountSelector := false
			switch typedSelector := parsedSelector.(type) {
			case *countSelector:
				isCountSelector = true
			case *aliasedSelector:
				_, ok := typedSelector.selector.(*countSelector)
				isCountSelector = ok
			}
			if !isCountSelector {
				rows[0][i] = nil
			}
		}
	}

	return EncodeRowsResult(genericTypeCodec, version, columns, rows)
}

func addColumnHelper(
	isStarSelector bool, first bool, newRow *[]interface{}, resultColumnMetadata *[]*message.ColumnMetadata,
	newColumnMetadata *message.ColumnMetadata, colExists bool, newColumnValue interface{}) error {
	if first {
		if colExists {
			*resultColumnMetadata = append(*resultColumnMetadata, newColumnMetadata)
			*newRow = append(*newRow, convertNullableToValue(newColumnValue))
		} else if !isStarSelector {
			return fmt.Errorf("could not find value for column %s", newColumnMetadata.Name)
		}
	} else {
		newColumnValueIndex := len(*newRow)
		if newColumnValueIndex >= len(*resultColumnMetadata) {
			return fmt.Errorf("column length mismatch between hosts: "+
				"resultColumnMetadata.len=%v newColumnValueIndex=%v name=%v", len(*resultColumnMetadata), newColumnValueIndex, newColumnMetadata.Name)
		}

		columnInResultMetadata := (*resultColumnMetadata)[newColumnValueIndex]
		if columnInResultMetadata.Name != newColumnMetadata.Name {
			return fmt.Errorf("column metadata mismatch between hosts: "+
				"columnInResultMetadata=%v newColumnMetadata=%v", columnInResultMetadata, newColumnMetadata.Name)
		}

		if colExists {
			*newRow = append(*newRow, convertNullableToValue(newColumnValue))
		} else if !isStarSelector {
			return fmt.Errorf("could not find value for column %s", newColumnMetadata.Name)
		}
	}
	return nil
}

func addColumnIfExists(isStarSelector bool, first bool, row *[]interface{}, columns *[]*message.ColumnMetadata,
	columnMetadata *message.ColumnMetadata, col *optionalColumn) error {
	return addColumnHelper(isStarSelector, first, row, columns, columnMetadata, col.exists, col.column)
}

func overrideColumnIfExists(isStarSelector bool, first bool, row *[]interface{}, columns *[]*message.ColumnMetadata,
	columnMetadata *message.ColumnMetadata, col *optionalColumn, val interface{}) error {
	return addColumnHelper(isStarSelector, first, row, columns, columnMetadata, col.exists, val)
}

func addColumn(isStarSelector bool, first bool, row *[]interface{}, columns *[]*message.ColumnMetadata,
	columnMetadata *message.ColumnMetadata, val interface{}) error {
	return addColumnHelper(isStarSelector, first, row, columns, columnMetadata, true, val)
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
