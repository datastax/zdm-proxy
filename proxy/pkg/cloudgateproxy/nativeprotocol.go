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
	return &ParsedRow {
		ColumnIndexes: columnIndexes,
		Columns: columns,
		Values:  values,
	}
}

type ParsedRowSet struct {
	ColumnIndexes     map[string]int
	Columns           []*message.ColumnMetadata
	PagingState       []byte
	Rows              []*ParsedRow
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

cqlsh> describe system.peers;

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
 */