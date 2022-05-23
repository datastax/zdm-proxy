package cloudgateproxy

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync/atomic"
)

type forwardDecision string

const (
	forwardToOrigin    = forwardDecision("origin")
	forwardToTarget    = forwardDecision("target")
	forwardToBoth      = forwardDecision("both")
	forwardToNone      = forwardDecision("none")
	forwardToAsyncOnly = forwardDecision("async") // for "synchronous" requests that should be sent to the async connector (handshake requests)
)

type interceptedQueryType string

const (
	peersV2 = interceptedQueryType("peersV2")
	peersV1 = interceptedQueryType("peersV1")
	local   = interceptedQueryType("local")
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

func buildStatementInfo(
	frameContext *frameDecodeContext,
	stmtsReplacedTerms []*statementReplacedTerms,
	psCache *PreparedStatementCache,
	mh *metrics.MetricHandler,
	currentKeyspaceName *atomic.Value,
	forwardReadsToTarget bool,
	forwardSystemQueriesToTarget bool,
	virtualizationEnabled bool,
	forwardAuthToTarget bool,
	timeUuidGenerator TimeUuidGenerator) (StatementInfo, error) {

	f := frameContext.GetRawFrame()
	switch f.Header.OpCode {
	case primitive.OpCodeQuery:
		stmtQueryData, err := frameContext.GetOrInspectStatement(timeUuidGenerator)
		if err != nil {
			return nil, fmt.Errorf("could not inspect QUERY frame: %w", err)
		}
		return getStatementInfoFromQueryInfo(
			frameContext.GetRawFrame(), currentKeyspaceName, forwardReadsToTarget,
			forwardSystemQueriesToTarget, virtualizationEnabled, stmtQueryData.queryData), nil
	case primitive.OpCodePrepare:
		stmtQueryData, err := frameContext.GetOrInspectStatement(timeUuidGenerator)
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
		baseStmtInfo := getStatementInfoFromQueryInfo(
			frameContext.GetRawFrame(), currentKeyspaceName, forwardReadsToTarget,
			forwardSystemQueriesToTarget, virtualizationEnabled, stmtQueryData.queryData)
		replacedTerms := make([]*term, 0)
		if len(stmtsReplacedTerms) > 1 {
			return nil, fmt.Errorf("expected single list of replaced terms for prepare message but got %v", len(stmtsReplacedTerms))
		} else if len(stmtsReplacedTerms) == 1 {
			replacedTerms = stmtsReplacedTerms[0].replacedTerms
		}
		return NewPreparedStatementInfo(baseStmtInfo, replacedTerms, stmtQueryData.queryData.hasPositionalBindMarkers(), prepareMsg.Query, prepareMsg.Keyspace), nil
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
			switch queryOrId := child.QueryOrId.(type) {
			case []byte:
				preparedData, err := getPreparedData(psCache, mh, queryOrId, primitive.OpCodeBatch, decodedFrame)
				if err != nil {
					return nil, err
				} else {
					preparedDataByStmtIdxMap[childIdx] = preparedData
				}
			default:
			}
		}
		return NewBatchStatementInfo(preparedDataByStmtIdxMap), nil
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
			return NewBoundStatementInfo(preparedData), nil
		}
	case primitive.OpCodeAuthResponse:
		if forwardAuthToTarget {
			return NewGenericStatementInfo(forwardToTarget, false), nil
		} else {
			return NewGenericStatementInfo(forwardToOrigin, false), nil
		}
	case primitive.OpCodeRegister, primitive.OpCodeStartup:
		return NewGenericStatementInfo(forwardToBoth, false), nil
	default:
		return NewGenericStatementInfo(forwardToBoth, true), nil
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

func getStatementInfoFromQueryInfo(
	f *frame.RawFrame,
	currentKeyspaceName *atomic.Value,
	forwardReadsToTarget bool,
	forwardSystemQueriesToTarget bool,
	virtualizationEnabled bool,
	queryInfo QueryInfo) StatementInfo {

	var sendAlsoToAsync bool
	forwardDecision := forwardToBoth
	if queryInfo.getStatementType() == statementTypeSelect {
		if virtualizationEnabled {
			if isSystemLocal(queryInfo, currentKeyspaceName) {
				log.Debugf("Detected system local query: %v with stream id: %v", queryInfo.getQuery(), f.Header.StreamId)
				return NewInterceptedStatementInfo(local)
			} else if isSystemPeersV1(queryInfo, currentKeyspaceName) {
				log.Debugf("Detected system peers query: %v with stream id: %v", queryInfo.getQuery(), f.Header.StreamId)
				return NewInterceptedStatementInfo(peersV1)
			} else if isSystemPeersV2(queryInfo, currentKeyspaceName) {
				log.Debugf("Detected system peers_v2 query: %v with stream id: %v", queryInfo.getQuery(), f.Header.StreamId)
				return NewInterceptedStatementInfo(peersV2)
			}
		}

		sendAlsoToAsync = true
		if isSystemQuery(queryInfo, currentKeyspaceName) {
			log.Debugf("Detected system query: %v with stream id: %v", queryInfo.getQuery(), f.Header.StreamId)
			if forwardSystemQueriesToTarget {
				forwardDecision = forwardToTarget
			} else {
				forwardDecision = forwardToOrigin
			}
		} else {
			if forwardReadsToTarget {
				forwardDecision = forwardToTarget
			} else {
				forwardDecision = forwardToOrigin
			}
		}
	} else if queryInfo.getStatementType() == statementTypeUse {
		sendAlsoToAsync = true
	} else {
		sendAlsoToAsync = false
	}

	log.Tracef("Forward decision: %s", forwardDecision)

	return NewGenericStatementInfo(forwardDecision, sendAlsoToAsync)
}

func isSystemQuery(info QueryInfo, currentKeyspaceName *atomic.Value) bool {
	keyspaceName := info.getKeyspaceName()
	if keyspaceName == "" {
		value := currentKeyspaceName.Load()
		if value != nil {
			keyspaceName = value.(string)
		}
	}

	return keyspaceName == "system" ||
		strings.HasPrefix(keyspaceName, "system_") ||
		strings.HasPrefix(keyspaceName, "dse_")
}

func isSystemPeersV1(info QueryInfo, currentKeyspaceName *atomic.Value) bool {
	keyspaceName := info.getKeyspaceName()
	if keyspaceName == "" {
		value := currentKeyspaceName.Load()
		if value != nil {
			keyspaceName = value.(string)
		}
	}

	return keyspaceName == "system" && info.getTableName() == "peers"
}

func isSystemPeersV2(info QueryInfo, currentKeyspaceName *atomic.Value) bool {
	keyspaceName := info.getKeyspaceName()
	if keyspaceName == "" {
		value := currentKeyspaceName.Load()
		if value != nil {
			keyspaceName = value.(string)
		}
	}

	return keyspaceName == "system" && info.getTableName() == "peers_v2"
}

func isSystemLocal(info QueryInfo, currentKeyspaceName *atomic.Value) bool {
	keyspaceName := info.getKeyspaceName()
	if keyspaceName == "" {
		value := currentKeyspaceName.Load()
		if value != nil {
			keyspaceName = value.(string)
		}
	}

	return keyspaceName == "system" && info.getTableName() == "local"
}

type frameDecodeContext struct {
	frame               *frame.RawFrame       // always non nil
	decodedFrame        *frame.Frame          // nil until first decode
	statementsQueryData []*statementQueryData // nil until first query inspection
}

var NotInspectableErr = errors.New("only Query and Prepare messages can be inspected")

func NewFrameDecodeContext(f *frame.RawFrame) *frameDecodeContext {
	return &frameDecodeContext{frame: f}
}

func NewInitializedFrameDecodeContext(f *frame.RawFrame, decodedFrame *frame.Frame, statementsQueryData []*statementQueryData) *frameDecodeContext {
	return &frameDecodeContext{
		frame:               f,
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

	decodedFrame, err := defaultCodec.ConvertFromRawFrame(recv.frame)
	if err != nil {
		return nil, fmt.Errorf("could not decode raw frame: %w", err)
	}

	recv.decodedFrame = decodedFrame
	return decodedFrame, nil
}

func (recv *frameDecodeContext) GetOrInspectStatement(timeUuidGenerator TimeUuidGenerator) (*statementQueryData, error) {
	err := recv.inspectStatements(timeUuidGenerator)
	if err != nil {
		return nil, err
	}

	if len(recv.statementsQueryData) != 1 {
		return nil, fmt.Errorf("expected 1 query info object but got %v", len(recv.statementsQueryData))
	}

	return recv.statementsQueryData[0], nil
}

func (recv *frameDecodeContext) GetOrInspectAllStatements(timeUuidGenerator TimeUuidGenerator) ([]*statementQueryData, error) {
	err := recv.inspectStatements(timeUuidGenerator)
	if err != nil {
		return nil, err
	}

	return recv.statementsQueryData, nil
}

func (recv *frameDecodeContext) inspectStatements(timeUuidGenerator TimeUuidGenerator) error {
	if recv.statementsQueryData != nil {
		return nil
	}

	decodedFrame, err := recv.GetOrDecodeFrame()
	if err != nil {
		return fmt.Errorf("could not decode frame: %w", err)
	}

	var statementsQueryData []*statementQueryData
	switch decodedFrame.Header.OpCode {
	case primitive.OpCodeQuery:
		queryMsg, ok := decodedFrame.Body.Message.(*message.Query)

		log.Tracef("Decoded frame %v", decodedFrame)

		if !ok {
			return fmt.Errorf("expected Query but got %v instead", decodedFrame.Body.Message.GetOpCode().String())
		}
		statementsQueryData = []*statementQueryData{&statementQueryData{statementIndex: 0, queryData: inspectCqlQuery(queryMsg.Query, timeUuidGenerator)}}
	case primitive.OpCodePrepare:
		prepareMsg, ok := decodedFrame.Body.Message.(*message.Prepare)
		if !ok {
			return fmt.Errorf("expected Prepare but got %v instead", decodedFrame.Body.Message.GetOpCode().String())
		}
		statementsQueryData = []*statementQueryData{&statementQueryData{statementIndex: 0, queryData: inspectCqlQuery(prepareMsg.Query, timeUuidGenerator)}}
	case primitive.OpCodeBatch:
		batchMsg, ok := decodedFrame.Body.Message.(*message.Batch)
		if !ok {
			return fmt.Errorf("expected Batch but got %v instead", decodedFrame.Body.Message.GetOpCode().String())
		}
		for idx, childStmt := range batchMsg.Children {
			switch typedQueryOrId := childStmt.QueryOrId.(type) {
			case string:
				statementsQueryData = append(statementsQueryData, &statementQueryData{statementIndex: idx, queryData: inspectCqlQuery(typedQueryOrId, timeUuidGenerator)})
			}
		}
	default:
		return fmt.Errorf("%v messages are not inspectable: %w", decodedFrame.Header.OpCode.String(), NotInspectableErr)
	}

	recv.statementsQueryData = statementsQueryData
	return nil
}

func (recv *frameDecodeContext) GetOrDecodeAndInspect(timeUuidGenerator TimeUuidGenerator) (*frame.Frame, []*statementQueryData, error) {
	decodedFrame, err := recv.GetOrDecodeFrame()
	if err != nil {
		return nil, nil, err
	}

	stmtsQueryData, err := recv.GetOrInspectAllStatements(timeUuidGenerator)
	if err != nil {
		return nil, nil, err
	}

	return decodedFrame, stmtsQueryData, nil
}