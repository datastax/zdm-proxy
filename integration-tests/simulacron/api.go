package simulacron

import (
	"fmt"
	"github.com/gocql/gocql"
	"time"
)

type When interface {
	ThenAlreadyExists(keyspace string, table string) Then
	ThenRowsSuccess(rowsResult *RowsResult) Then
	ThenSuccess() Then
	ThenServerError(serverError ServerError, message string) Then
	ThenWriteTimeout(consistencyLevel gocql.Consistency, received int, blockFor int, writeType WriteType) Then
	ThenReadTimeout(consistencyLevel gocql.Consistency, received int, blockFor int, dataPresent bool) Then
}

type baseWhen struct {
	out map[string]interface{}
}

type ServerError string
type DataType string

type WriteType string

const (
	OverloadedError     = ServerError("overloaded")
	IsBootstrapping     = ServerError("is_bootstrapping")
	Invalid             = ServerError("invalid")
	ConfigError         = ServerError("config_error")
	ProtocolError       = ServerError("protocol_error")
	SyntaxError         = ServerError("syntax_error")
	TruncateError       = ServerError("truncate_error")
	Unauthorized        = ServerError("unauthorized")
	Unprepared          = ServerError("unprepared")
	AuthenticationError = ServerError("authentication_error")
)

const (
	DataTypeText      = DataType("ascii") // simulacron doesn't support 'text'
	DataTypeInt       = DataType("int")
	DataTypeBigInt    = DataType("bigint")
	DataTypeBlob      = DataType("blob")
	DataTypeBoolean   = DataType("boolean")
	DataTypeCounter   = DataType("counter")
	DataTypeDecimal   = DataType("decimal")
	DataTypeDouble    = DataType("double")
	DataTypeFloat     = DataType("float")
	DataTypeTimestamp = DataType("timestamp")
	DataTypeUuid      = DataType("uuid")
	DataTypeVarchar   = DataType("varchar")
	DataTypeVarInt    = DataType("varint")
	DataTypeTimeuuid  = DataType("timeuuid")
	DataTypeInet      = DataType("inet")
	DataTypeDate      = DataType("date")
	DataTypeTime      = DataType("time")
	DataTypeEmpty     = DataType("empty")
	DataTypeSmallint  = DataType("smallint")
	DataTypeTinyInt   = DataType("tinyint")
	DataTypeDuration  = DataType("duration")
)

const (
	Cdc           = WriteType("CDC")
	Cas           = WriteType("CAS")
	UnloggedBatch = WriteType("UNLOGGED_BATCH")
	Batch         = WriteType("BATCH")
	Counter       = WriteType("COUNTER")
	BatchLog      = WriteType("BATCH_LOG")
	Simple        = WriteType("SIMPLE")
	View          = WriteType("VIEW")
)

type queryParameter struct {
	name     string
	dataType DataType
	value    interface{}
}

type WhenQueryOptions struct {
	values           []queryParameter
	consistencyLevel *gocql.Consistency
}

func WhenQuery(cql string, options *WhenQueryOptions) When {
	out := map[string]interface{}{
		"request": "query",
		"query":   cql,
	}

	if len(options.values) != 0 {
		parameters := make(map[string]interface{})
		paramTypes := make(map[string]DataType)
		for _, param := range options.values {
			parameters[param.name] = param.value
			paramTypes[param.name] = param.dataType
		}

		out["params"] = parameters
		out["param_types"] = paramTypes
	}

	if options.consistencyLevel != nil {
		out["consistency_level"] = *options.consistencyLevel
	}

	return when(out)
}

func NewWhenQueryOptions() *WhenQueryOptions {
	return &WhenQueryOptions{}
}

func (queryOptions *WhenQueryOptions) WithNamedParameter(name string, dataType DataType, value interface{}) *WhenQueryOptions {
	queryOptions.values = append(queryOptions.values, queryParameter{
		name:     name,
		dataType: dataType,
		value:    value,
	})
	return queryOptions
}

func (queryOptions *WhenQueryOptions) WithPositionalParameter(dataType DataType, value interface{}) *WhenQueryOptions {
	return queryOptions.WithNamedParameter(fmt.Sprintf("column%d", len(queryOptions.values)), dataType, value)
}

func (queryOptions *WhenQueryOptions) WithConsistencyLevel(consistencyLevel gocql.Consistency) *WhenQueryOptions {
	queryOptions.consistencyLevel = &consistencyLevel
	return queryOptions
}

type WhenBatchOptions struct {
	batchQueries             []*BatchQuery
	allowedConsistencyLevels []gocql.Consistency
}

func WhenBatch(options *WhenBatchOptions) When {
	out := map[string]interface{}{
		"request": "batch",
	}

	queries := make([]map[string]interface{}, len(options.batchQueries))
	for i, query := range options.batchQueries {
		queries[i] = query.render()
	}
	out["queries"] = queries

	consistencyLevels := make([]string, len(options.allowedConsistencyLevels))
	for i, cl := range options.allowedConsistencyLevels {
		consistencyLevels[i] = cl.String()
	}
	out["consistency_level"] = consistencyLevels

	return when(out)
}

func NewWhenBatchOptions() *WhenBatchOptions {
	return &WhenBatchOptions{}
}

func (batchOptions *WhenBatchOptions) WithQueries(queries ...*BatchQuery) *WhenBatchOptions {
	batchOptions.batchQueries = queries
	return batchOptions
}

func (batchOptions *WhenBatchOptions) WithAllowedConsistencyLevels(consistencyLevels ...gocql.Consistency) *WhenBatchOptions {
	batchOptions.allowedConsistencyLevels = consistencyLevels
	return batchOptions
}

type BatchQuery struct {
	queryOrId string
	values    []queryParameter
}

func NewBatchQuery(queryOrId string) *BatchQuery {
	return &BatchQuery{
		queryOrId: queryOrId,
		values:    nil,
	}
}

func (batchQuery *BatchQuery) WithNamedParameter(name string, dataType DataType, value interface{}) *BatchQuery {
	batchQuery.values = append(batchQuery.values, queryParameter{
		name:     name,
		dataType: dataType,
		value:    value,
	})
	return batchQuery
}

func (batchQuery *BatchQuery) WithPositionalParameter(dataType DataType, value interface{}) *BatchQuery {
	return batchQuery.WithNamedParameter(fmt.Sprintf("column%d", len(batchQuery.values)), dataType, value)
}

func (batchQuery *BatchQuery) render() map[string]interface{} {
	out := map[string]interface{}{
		"query": batchQuery.queryOrId,
	}

	if len(batchQuery.values) == 0 {
		return out
	}

	parameters := make(map[string]interface{})
	paramTypes := make(map[string]DataType)
	for _, param := range batchQuery.values {
		parameters[param.name] = param.value
		paramTypes[param.name] = param.dataType
	}

	out["params"] = parameters
	out["param_types"] = paramTypes
	return out
}

func (when *baseWhen) render() map[string]interface{} {
	return when.out
}

type Then interface {
	render() map[string]interface{}
	WithIgnoreOnPrepare(ignoreOnPrepare bool) Then
	WithDelay(delay time.Duration) Then
}

type baseThen struct {
	out             map[string]interface{}
	ignoreOnPrepare bool
	delay           time.Duration
	when            *baseWhen
}

func (then *baseThen) render() map[string]interface{} {
	out := then.out
	out["delay_in_ms"] = then.delay.Milliseconds()
	out["ignore_on_prepare"] = then.ignoreOnPrepare

	return map[string]interface{}{
		"when": then.when.render(),
		"then": out,
	}
}

func (then *baseThen) WithIgnoreOnPrepare(ignoreOnPrepare bool) Then {
	then.ignoreOnPrepare = ignoreOnPrepare
	return then
}

func (then *baseThen) WithDelay(delay time.Duration) Then {
	then.delay = delay
	return then
}

type RowsResult struct {
	columnNamesToTypes map[string]DataType
	rows               []map[string]interface{}
}

func NewRowsResult(columnNamesToTypes map[string]DataType) *RowsResult {
	return &RowsResult{columnNamesToTypes: columnNamesToTypes}
}

func (rowsResult *RowsResult) WithRows(rows ...map[string]interface{}) *RowsResult {
	rowsResult.rows = append(rowsResult.rows, rows...)
	return rowsResult
}

func (rowsResult *RowsResult) WithRow(row map[string]interface{}) *RowsResult {
	rowsResult.rows = append(rowsResult.rows, row)
	return rowsResult
}

func (when *baseWhen) ThenRowsSuccess(rowsResult *RowsResult) Then {
	paramTypes := make(map[string]interface{})
	for name, dataType := range rowsResult.columnNamesToTypes {
		paramTypes[name] = string(dataType)
	}
	return when.then(map[string]interface{}{
		"result":       "success",
		"rows":         rowsResult.rows,
		"column_types": paramTypes,
	})
}

func (when *baseWhen) ThenServerError(serverError ServerError, message string) Then {
	return when.then(map[string]interface{}{
		"result":  string(serverError),
		"message": message,
	})
}

func (when *baseWhen) ThenSuccess() Then {
	return when.then(map[string]interface{}{
		"result": "success",
	})
}

func (when *baseWhen) ThenWriteTimeout(
	consistencyLevel gocql.Consistency, received int, blockFor int, writeType WriteType) Then {
	return when.then(map[string]interface{}{
		"result":            "write_timeout",
		"consistency_level": consistencyLevel,
		"received":          received,
		"block_for":         blockFor,
		"message":           "Write timed out",
		"write_type":        string(writeType),
	})
}

func (when *baseWhen) ThenReadTimeout(
	consistencyLevel gocql.Consistency, received int, blockFor int, dataPresent bool) Then {
	return when.then(map[string]interface{}{
		"result":            "read_timeout",
		"consistency_level": consistencyLevel,
		"received":          received,
		"block_for":         blockFor,
		"message":           "Read timed out",
		"data_present":      dataPresent,
	})
}

func (when *baseWhen) ThenAlreadyExists(keyspace string, table string) Then {
	return when.then(map[string]interface{}{
		"result":   "already_exists",
		"message":  "already_exists",
		"keyspace": keyspace,
		"table":    table,
	})
}

func (when *baseWhen) then(out map[string]interface{}) Then {
	then := &baseThen{}
	then.out = out
	then.when = when
	return then
}

func when(out map[string]interface{}) When {
	when := &baseWhen{}
	when.out = out
	return when
}
