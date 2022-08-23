package simulacron

import (
	"encoding/json"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type ClusterLogs struct {
	Id          int               `json:"id"`
	Datacenters []*DatacenterLogs `json:"data_centers"`
}

type DatacenterLogs struct {
	Id    int         `json:"id"`
	Nodes []*NodeLogs `json:"nodes"`
}

type NodeLogs struct {
	Id      int                `json:"id"`
	Queries []*RequestLogEntry `json:"queries"`
}

type RequestLogEntry struct {
	Query                  string         `json:"query"`
	ConsistencyLevel       string         `json:"consistency_level"`
	SerialConsistencyLevel string         `json:"serial_consistency_level"`
	Connection             string         `json:"connection"`
	ReceivedTimestamp      int64          `json:"received_timestamp"`
	ClientTimestamp        int64          `json:"client_timestamp"`
	Primed                 bool           `json:"primed"`
	QueryType              QueryType      `json:"type"`
	Frame                  *FrameLogEntry `json:"frame"`
}

type FrameLogEntry struct {
	ProtocolVersion primitive.ProtocolVersion `json:"protocol_version"`
	Beta            bool                      `json:"beta"`
	StreamId        int16                     `json:"stream_id"`
	TracingId       string                    `json:"tracing_id"`
	CustomPayload   map[string]string         `json:"custom_payload"`
	Warnings        []string                  `json:"warnings"`
	Message         json.RawMessage           `json:"message"`
}

type BatchMessage struct {
	Type              string           `json:"type"`
	Opcode            primitive.OpCode `json:"opcode"`
	IsResponse        bool             `json:"is_response"`
	QueriesOrIds      []string         `json:"queries_or_ids"`
	Values            [][]string       `json:"values"`
	Consistency       string           `json:"consistency"`
	SerialConsistency string           `json:"serial_consistency"`
	DefaultTimestamp  int64            `json:"default_timestamp"`
	Keyspace          string           `json:"keyspace"`
}

type ExecuteMessage struct {
	Type             string           `json:"type"`
	Opcode           primitive.OpCode `json:"opcode"`
	ResultMetadataId string           `json:"result_metadata_id"`
	IsResponse       bool             `json:"is_response"`
	Options          *QueryOptions    `json:"options"`
	Id               string           `json:"id"`
}

type QueryOptions struct {
	Consistency       string            `json:"consistency"`
	PositionalValues  []string          `json:"positional_values"`
	NamedValues       map[string]string `json:"named_values"`
	Keyspace          string            `json:"keyspace"`
	SkipMetadata      bool              `json:"skip_metadata"`
	SerialConsistency string            `json:"serial_consistency"`
	DefaultTimestamp  int64             `json:"default_timestamp"`
	PageSize          int               `json:"page_size"`
	PagingState       string            `json:"paging_state"`
}

type QueryType string

const (
	QueryTypeQuery    QueryType = "QUERY"
	QueryTypeExecute  QueryType = "EXECUTE"
	QueryTypeBatch    QueryType = "BATCH"
	QueryTypePrepare  QueryType = "PREPARE"
	QueryTypeOptions  QueryType = "OPTIONS"
	QueryTypeStartup  QueryType = "STARTUP"
	QueryTypeRegister QueryType = "REGISTER"
)

func (baseSimulacron *baseSimulacron) DeleteLogs() error {
	_, err := baseSimulacron.process.execHttp("DELETE", baseSimulacron.getPath("log"), nil)
	return err
}

func (baseSimulacron *baseSimulacron) GetLogs() (*ClusterLogs, error) {
	bytes, err := baseSimulacron.process.execHttp("GET", baseSimulacron.getPath("log"), nil)
	if err != nil {
		return nil, err
	}

	var clusterLogs ClusterLogs
	err = json.Unmarshal(bytes, &clusterLogs)
	if err != nil {
		return nil, err
	}

	return &clusterLogs, nil
}

func (baseSimulacron *baseSimulacron) GetLogsByType(queryType QueryType) (*ClusterLogs, error) {
	return baseSimulacron.GetLogsWithFilter(func(entry *RequestLogEntry) bool {
		return entry.QueryType == queryType
	})
}

func (baseSimulacron *baseSimulacron) GetLogsWithFilter(filterFunc func(*RequestLogEntry) bool) (*ClusterLogs, error) {
	logs, err := baseSimulacron.GetLogs()
	if err != nil {
		return nil, err
	}

	for _, dc := range logs.Datacenters {
		for _, node := range dc.Nodes {
			var queries []*RequestLogEntry
			for _, query := range node.Queries {
				if filterFunc(query) {
					queries = append(queries, query)
				}
			}
			node.Queries = queries
		}
	}

	return logs, nil
}
