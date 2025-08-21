package zdmproxy

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"strings"
)

type forwardDecision string

const (
	forwardToOrigin    = forwardDecision("origin")
	forwardToTarget    = forwardDecision("target")
	forwardToBoth      = forwardDecision("both")
	forwardToNone      = forwardDecision("none")
	forwardToAsyncOnly = forwardDecision("async") // for "synchronous" requests that should be sent to the async connector (handshake requests)
)

const (
	insightsRpcName = "InsightsRpc"
)

type interceptedQueryType string

const (
	peersV2 = interceptedQueryType("peersV2")
	peersV1 = interceptedQueryType("peersV1")
	local   = interceptedQueryType("local")
)

const (
	systemPeersTableName   = "peers"
	systemPeersV2TableName = "peers_v2"
	systemLocalTableName   = "local"
	systemKeyspaceName     = "system"
	nowFunctionName        = "now"
)

type UnpreparedExecuteError struct {
	Header     *frame.Header
	Body       *frame.Body
	preparedId []byte
}

type statementQueryData struct {
	statementIndex int
	queryData      QueryInfo
}

type statementReplacedTerms struct {
	statementIndex int
	replacedTerms  []*term
}

func (uee *UnpreparedExecuteError) Error() string {
	return fmt.Sprintf("The preparedID of the statement to be executed (%s) does not exist in the proxy cache", hex.EncodeToString(uee.preparedId))
}

func buildRequestInfo(
	frameContext *frameDecodeContext,
	stmtsReplacedTerms []*statementReplacedTerms,
	psCache *PreparedStatementCache,
	mh *metrics.MetricHandler,
	currentKeyspaceName string,
	primaryCluster common.ClusterType,
	forwardSystemQueriesToTarget bool,
	virtualizationEnabled bool,
	forwardAuthToTarget bool,
	timeUuidGenerator TimeUuidGenerator) (RequestInfo, error) {

	f := frameContext.GetRawFrame()
	switch f.Header.OpCode {
	case primitive.OpCodeQuery:
		stmtQueryData, err := frameContext.GetOrInspectStatement(currentKeyspaceName, timeUuidGenerator)
		if err != nil {
			return nil, fmt.Errorf("could not inspect QUERY frame: %w", err)
		}
		return getRequestInfoFromQueryInfo(
			frameContext.GetRawFrame(), primaryCluster,
			forwardSystemQueriesToTarget, virtualizationEnabled, stmtQueryData.queryData), nil
	case primitive.OpCodePrepare:
		stmtQueryData, err := frameContext.GetOrInspectStatement(currentKeyspaceName, timeUuidGenerator)
		if err != nil {
			return nil, fmt.Errorf("could not inspect PREPARE frame: %w", err)
		}
		decodedFrame, err := frameContext.GetOrDecodeFrame()
		if err != nil {
			return nil, fmt.Errorf("could not decode frame: %w", err)
		}
		prepareMsg, ok := decodedFrame.Body.Message.(*message.Prepare)
		if !ok {
			return nil, fmt.Errorf("unexpected message type when decoding PREPARE message: %v", decodedFrame.Body.Message)
		}
		baseRequestInfo := getRequestInfoFromQueryInfo(
			frameContext.GetRawFrame(), primaryCluster,
			forwardSystemQueriesToTarget, virtualizationEnabled, stmtQueryData.queryData)
		replacedTerms := make([]*term, 0)
		if len(stmtsReplacedTerms) > 1 {
			return nil, fmt.Errorf("expected single list of replaced terms for prepare message but got %v", len(stmtsReplacedTerms))
		} else if len(stmtsReplacedTerms) == 1 {
			replacedTerms = stmtsReplacedTerms[0].replacedTerms
		}
		return NewPrepareRequestInfo(baseRequestInfo, replacedTerms, stmtQueryData.queryData.hasPositionalBindMarkers(), prepareMsg.Query, prepareMsg.Keyspace), nil
	case primitive.OpCodeBatch:
		decodedFrame, err := frameContext.GetOrDecodeFrame()
		if err != nil {
			return nil, fmt.Errorf("could not decode batch raw frame: %w", err)
		}
		batchMsg, ok := decodedFrame.Body.Message.(*message.Batch)
		if !ok {
			return nil, fmt.Errorf("could not convert message with batch op code to batch type, got %v instead", decodedFrame.Body.Message)
		}
		preparedDataByStmtIdxMap := make(map[int]PreparedData)
		for childIdx, child := range batchMsg.Children {
			if child.Id != nil {
				preparedData, err := getPreparedData(psCache, mh, child.Id, primitive.OpCodeBatch, decodedFrame)
				if err != nil {
					return nil, err
				} else {
					preparedDataByStmtIdxMap[childIdx] = preparedData
				}
			}
		}
		return NewBatchRequestInfo(preparedDataByStmtIdxMap), nil
	case primitive.OpCodeExecute:
		decodedFrame, err := frameContext.GetOrDecodeFrame()
		if err != nil {
			return nil, fmt.Errorf("could not decode execute raw frame: %w", err)
		}
		executeMsg, ok := decodedFrame.Body.Message.(*message.Execute)
		if !ok {
			return nil, fmt.Errorf("expected Execute but got %v instead", decodedFrame.Body.Message.GetOpCode())
		}
		preparedData, err := getPreparedData(psCache, mh, executeMsg.QueryId, primitive.OpCodeExecute, decodedFrame)
		if err != nil {
			return nil, err
		} else {
			return NewExecuteRequestInfo(preparedData), nil
		}
	case primitive.OpCodeAuthResponse:
		if forwardAuthToTarget {
			return NewGenericRequestInfo(forwardToTarget, false, false), nil
		} else {
			return NewGenericRequestInfo(forwardToOrigin, false, false), nil
		}
	case primitive.OpCodeRegister, primitive.OpCodeStartup:
		return NewGenericRequestInfo(forwardToBoth, false, false), nil
	default:
		return NewGenericRequestInfo(forwardToBoth, true, false), nil
	}
}

func getPreparedData(
	psCache *PreparedStatementCache,
	mh *metrics.MetricHandler,
	preparedId []byte,
	code primitive.OpCode,
	decodedFrame *frame.Frame) (PreparedData, error) {
	if preparedData, ok := psCache.Get(preparedId); ok {
		log.Tracef("%v with prepared-id = '%s' has prepared-data = %v", code.String(), hex.EncodeToString(preparedId), preparedData)
		// The forward decision was set in the cache when handling the corresponding PREPARE request
		return preparedData, nil
	} else {
		log.Warnf("No cached entry for prepared-id = '%s' for %v.", hex.EncodeToString(preparedId), code.String())
		mh.GetProxyMetrics().PSCacheMissCount.Add(1)
		// return meaningful error to caller so it can generate an unprepared response
		return nil, &UnpreparedExecuteError{Header: decodedFrame.Header, Body: decodedFrame.Body, preparedId: preparedId}
	}
}

func getRequestInfoFromQueryInfo(
	f *frame.RawFrame,
	primaryCluster common.ClusterType,
	forwardSystemQueriesToTarget bool,
	virtualizationEnabled bool,
	queryInfo QueryInfo) RequestInfo {

	var sendAlsoToAsync bool
	forwardDecision := forwardToBoth
	trackMetrics := true
	if queryInfo.getStatementType() == statementTypeSelect {
		if virtualizationEnabled {
			parsedSelectClause := queryInfo.getParsedSelectClause()
			if isSystemLocal(queryInfo) {
				log.Debugf("Detected system local query: %v with stream id: %v", queryInfo.getQuery(), f.Header.StreamId)
				return NewInterceptedRequestInfo(local, parsedSelectClause)
			} else if isSystemPeersV1(queryInfo) {
				log.Debugf("Detected system peers query: %v with stream id: %v", queryInfo.getQuery(), f.Header.StreamId)
				return NewInterceptedRequestInfo(peersV1, parsedSelectClause)
			} else if isSystemPeersV2(queryInfo) {
				log.Debugf("Detected system peers_v2 query: %v with stream id: %v", queryInfo.getQuery(), f.Header.StreamId)
				return NewInterceptedRequestInfo(peersV2, parsedSelectClause)
			}
		}

		if isSystemQuery(queryInfo) {
			sendAlsoToAsync = false
			log.Debugf("Detected system query: %v with stream id: %v", queryInfo.getQuery(), f.Header.StreamId)
			if forwardSystemQueriesToTarget {
				forwardDecision = forwardToTarget
			} else {
				forwardDecision = forwardToOrigin
			}
		} else {
			sendAlsoToAsync = true
			if primaryCluster == common.ClusterTypeTarget {
				forwardDecision = forwardToTarget
			} else {
				forwardDecision = forwardToOrigin
			}
		}
	} else if queryInfo.getStatementType() == statementTypeUse {
		sendAlsoToAsync = true
	} else if queryInfo.getStatementType() == statementTypeCall {
		if strings.EqualFold(queryInfo.getCallRpcName(), insightsRpcName) {
			// ignore Insights client calls
			forwardDecision = forwardToNone
			trackMetrics = false
			sendAlsoToAsync = false
		}
	} else {
		sendAlsoToAsync = false
	}

	log.Tracef("Forward decision: %s", forwardDecision)

	return NewGenericRequestInfo(forwardDecision, sendAlsoToAsync, trackMetrics)
}

func isSystemQuery(info QueryInfo) bool {
	keyspace := info.getApplicableKeyspace()
	return isSystemKeyspace(keyspace) ||
		strings.HasPrefix(keyspace, "system_") ||
		strings.HasPrefix(keyspace, "dse_")
}

func isSystemPeersV1(info QueryInfo) bool {
	return isSystemKeyspace(info.getApplicableKeyspace()) && isPeersV1Table(info.getTableName())
}

func isPeersV1Table(tableName string) bool {
	return tableName == systemPeersTableName
}

func isSystemPeersV2(info QueryInfo) bool {
	return isSystemKeyspace(info.getApplicableKeyspace()) && isPeersV2Table(info.getTableName())
}

func isPeersV2Table(tableName string) bool {
	return tableName == systemPeersV2TableName
}

func isSystemLocal(info QueryInfo) bool {
	return isSystemKeyspace(info.getApplicableKeyspace()) && isLocalTable(info.getTableName())
}

func isLocalTable(tableName string) bool {
	return tableName == systemLocalTableName
}

func isSystemKeyspace(keyspace string) bool {
	return keyspace == systemKeyspaceName
}

type frameDecodeContext struct {
	frame               *frame.RawFrame // always non nil
	compression         primitive.Compression
	decodedFrame        *frame.Frame          // nil until first decode
	statementsQueryData []*statementQueryData // nil until first query inspection
}

var NotInspectableErr = errors.New("only Query and Prepare messages can be inspected")

func NewFrameDecodeContext(f *frame.RawFrame, compression primitive.Compression) *frameDecodeContext {
	return &frameDecodeContext{frame: f, compression: compression}
}

func NewInitializedFrameDecodeContext(f *frame.RawFrame, compression primitive.Compression, decodedFrame *frame.Frame, statementsQueryData []*statementQueryData) *frameDecodeContext {
	return &frameDecodeContext{
		frame:               f,
		compression:         compression,
		decodedFrame:        decodedFrame,
		statementsQueryData: statementsQueryData}
}

func (recv *frameDecodeContext) GetRawFrame() *frame.RawFrame {
	return recv.frame
}

func (recv *frameDecodeContext) GetOrDecodeFrame() (*frame.Frame, error) {
	if recv.decodedFrame != nil {
		return recv.decodedFrame, nil
	}

	if codec, ok := codecs[recv.compression]; ok {
		decodedFrame, err := codec.ConvertFromRawFrame(recv.frame)
		if err != nil {
			return nil, fmt.Errorf("could not decode raw frame: %w", err)
		}
		recv.decodedFrame = decodedFrame
		return decodedFrame, nil
	}
	return nil, fmt.Errorf("no codec for compression: %v", recv.compression)
}

func (recv *frameDecodeContext) GetOrInspectStatement(currentKeyspace string, timeUuidGenerator TimeUuidGenerator) (*statementQueryData, error) {
	err := recv.inspectStatements(currentKeyspace, timeUuidGenerator)
	if err != nil {
		return nil, err
	}

	if len(recv.statementsQueryData) != 1 {
		return nil, fmt.Errorf("expected 1 query info object but got %v", len(recv.statementsQueryData))
	}

	return recv.statementsQueryData[0], nil
}

func (recv *frameDecodeContext) GetOrInspectAllStatements(currentKeyspace string, timeUuidGenerator TimeUuidGenerator) ([]*statementQueryData, error) {
	err := recv.inspectStatements(currentKeyspace, timeUuidGenerator)
	if err != nil {
		return nil, err
	}

	return recv.statementsQueryData, nil
}

func (recv *frameDecodeContext) GetRequestId(conf *config.Config) []byte {
	decodedFrame, err := recv.GetOrDecodeFrame()
	if err != nil {
		return nil
	}
	if decodedFrame.Body == nil || decodedFrame.Body.CustomPayload == nil {
		return nil
	}
	return recv.decodedFrame.Body.CustomPayload[conf.TracingRequestIdKey]
}

func (recv *frameDecodeContext) inspectStatements(currentKeyspace string, timeUuidGenerator TimeUuidGenerator) error {
	if recv.statementsQueryData != nil {
		return nil
	}

	decodedFrame, err := recv.GetOrDecodeFrame()
	if err != nil {
		return fmt.Errorf("could not decode frame: %w", err)
	}

	var statementsQueryData []*statementQueryData
	switch typedMsg := decodedFrame.Body.Message.(type) {
	case *message.Query:
		log.Tracef("Decoded frame %v", decodedFrame)
		if protocolSupportsKeyspaceInRequest(decodedFrame.Header.Version) &&
			typedMsg.Options != nil &&
			typedMsg.Options.Flags().Contains(primitive.QueryFlagWithKeyspace) {
			currentKeyspace = typedMsg.Options.Keyspace
		}
		statementsQueryData = []*statementQueryData{
			{statementIndex: 0, queryData: inspectCqlQuery(typedMsg.Query, currentKeyspace, timeUuidGenerator)}}
	case *message.Prepare:
		if protocolSupportsKeyspaceInRequest(decodedFrame.Header.Version) &&
			typedMsg.Flags().Contains(primitive.PrepareFlagWithKeyspace) {
			currentKeyspace = typedMsg.Keyspace
		}
		statementsQueryData = []*statementQueryData{
			{statementIndex: 0, queryData: inspectCqlQuery(typedMsg.Query, currentKeyspace, timeUuidGenerator)}}
	case *message.Batch:
		if protocolSupportsKeyspaceInRequest(decodedFrame.Header.Version) &&
			typedMsg.Flags().Contains(primitive.QueryFlagWithKeyspace) {
			currentKeyspace = typedMsg.Keyspace
		}
		for idx, childStmt := range typedMsg.Children {
			if len(childStmt.Query) > 0 {
				statementsQueryData = append(
					statementsQueryData, &statementQueryData{
						statementIndex: idx, queryData: inspectCqlQuery(childStmt.Query, currentKeyspace, timeUuidGenerator)})
			}
		}
	default:
		return fmt.Errorf("%v messages are not inspectable: %w", decodedFrame.Header.OpCode.String(), NotInspectableErr)
	}

	recv.statementsQueryData = statementsQueryData
	return nil
}

func (recv *frameDecodeContext) GetOrDecodeAndInspect(currentKeyspace string, timeUuidGenerator TimeUuidGenerator) (*frame.Frame, []*statementQueryData, error) {
	decodedFrame, err := recv.GetOrDecodeFrame()
	if err != nil {
		return nil, nil, err
	}

	stmtsQueryData, err := recv.GetOrInspectAllStatements(currentKeyspace, timeUuidGenerator)
	if err != nil {
		return nil, nil, err
	}

	return decodedFrame, stmtsQueryData, nil
}

func protocolSupportsKeyspaceInRequest(v primitive.ProtocolVersion) bool {
	return v >= primitive.ProtocolVersion5 && v != primitive.ProtocolVersionDse1
}
