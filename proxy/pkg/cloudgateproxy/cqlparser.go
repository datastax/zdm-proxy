package cloudgateproxy

import (
	"encoding/hex"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/google/uuid"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync/atomic"
)

type forwardDecision string

const (
	forwardToOrigin = forwardDecision("origin")
	forwardToTarget = forwardDecision("target")
	forwardToBoth   = forwardDecision("both")
	forwardToNone   = forwardDecision("none")
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

func (uee *UnpreparedExecuteError) Error() string {
	return fmt.Sprintf("The preparedID of the statement to be executed (%s) does not exist in the proxy cache", hex.EncodeToString(uee.preparedId))
}

// modifyFrame modifies the incoming request in certain conditions:
//   * if the request is a QUERY and it contains now() function calls
func modifyFrame(context *frameDecodeContext) (*frameDecodeContext, error) {
	switch context.frame.Header.OpCode {
	case primitive.OpCodeQuery:
		decodedFrame, err := context.GetOrDecodeFrame()
		if err != nil {
			return nil, fmt.Errorf("could not decode raw frame of a QUERY request: %w", err)
		}
		queryInfo, err  := context.GetOrInspectQuery()
		if err != nil {
			return nil, fmt.Errorf("could not inspect query of a QUERY frame: %w", err)
		}
		if queryInfo.hasNowFunctionCalls() {
			timeUUID, err := uuid.NewUUID()
			if err != nil {
				return nil, fmt.Errorf("could not generate type 1 UUID values, can not modify query: %w", err)
			}
			newQueryInfo := queryInfo.replaceNowFunctionCallsWithLiteral(timeUUID)
			newQueryFrame := decodedFrame.Clone()
			newQueryMsg, ok := newQueryFrame.Body.Message.(*message.Query)
			if !ok {
				return nil, fmt.Errorf("expected Query in cloned frame but got %v instead", newQueryFrame.Body.Message.GetOpCode())
			}
			newQueryMsg.Query = newQueryInfo.getQuery()
			newQueryRawFrame, err := defaultCodec.ConvertToRawFrame(newQueryFrame)
			if err != nil {
				return nil, fmt.Errorf("could not convert modified query frame to raw frame: %w", err)
			}
			return &frameDecodeContext{
				frame:        newQueryRawFrame,
				decodedFrame: newQueryFrame,
				queryInfo:    newQueryInfo,
			}, nil
		} else {
			return context, nil
		}
	default:
		return context, nil
	}
}

func parseStatement(
	frameContext *frameDecodeContext,
	psCache *PreparedStatementCache,
	mh *metrics.MetricHandler,
	currentKeyspaceName *atomic.Value,
	forwardReadsToTarget bool,
	forwardSystemQueriesToTarget bool,
	virtualizationEnabled bool,
	forwardAuthToTarget bool) (StatementInfo, error) {

	f := frameContext.frame
	switch f.Header.OpCode {
	case primitive.OpCodeQuery:
		queryInfo, err := frameContext.GetOrInspectQuery()
		if err != nil {
			return nil, fmt.Errorf("could not inspect QUERY frame: %w", err)
		}
		return getStatementInfoFromQueryInfo(
			frameContext.frame, currentKeyspaceName, forwardReadsToTarget,
			forwardSystemQueriesToTarget, virtualizationEnabled, queryInfo), nil
	case primitive.OpCodePrepare:
		queryInfo, err := frameContext.GetOrInspectQuery()
		if err != nil {
			return nil, fmt.Errorf("could not inspect PREPARE frame: %w", err)
		}
		baseStmtInfo := getStatementInfoFromQueryInfo(
			frameContext.frame, currentKeyspaceName, forwardReadsToTarget,
			forwardSystemQueriesToTarget, virtualizationEnabled, queryInfo)
		return NewPreparedStatementInfo(baseStmtInfo), nil
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
			return NewGenericStatementInfo(forwardToTarget), nil
		} else {
			return NewGenericStatementInfo(forwardToOrigin), nil
		}
	default:
		return NewGenericStatementInfo(forwardToBoth), nil
	}
}

func getPreparedData(
	psCache *PreparedStatementCache,
	mh *metrics.MetricHandler,
	preparedId []byte,
	code primitive.OpCode,
	decodedFrame *frame.Frame) (PreparedData, error) {
	var requestType string
	switch code {
	case primitive.OpCodeBatch:
		requestType = "BATCH"
	case primitive.OpCodeExecute:
		requestType = "EXECUTE"
	default:
		requestType = "UNKNOWN"
		log.Warnf("Unknown op code when fetching prepared data, this is most likely a bug. OpCode = %v, Request = %v", code, decodedFrame)
	}

	if preparedData, ok := psCache.Get(preparedId); ok {
		log.Tracef("%v with prepared-id = '%s' has prepared-data = %v", requestType, hex.EncodeToString(preparedId), preparedData)
		// The forward decision was set in the cache when handling the corresponding PREPARE request
		return preparedData, nil
	} else {
		log.Warnf("No cached entry for prepared-id = '%s' while processing a %v.", hex.EncodeToString(preparedId), requestType)
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
	queryInfo queryInfo) StatementInfo {

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
	}

	return NewGenericStatementInfo(forwardDecision)
}

func isSystemQuery(info queryInfo, currentKeyspaceName *atomic.Value) bool {
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

func isSystemPeersV1(info queryInfo, currentKeyspaceName *atomic.Value) bool {
	keyspaceName := info.getKeyspaceName()
	if keyspaceName == "" {
		value := currentKeyspaceName.Load()
		if value != nil {
			keyspaceName = value.(string)
		}
	}

	return keyspaceName == "system" && info.getTableName() == "peers"
}

func isSystemPeersV2(info queryInfo, currentKeyspaceName *atomic.Value) bool {
	keyspaceName := info.getKeyspaceName()
	if keyspaceName == "" {
		value := currentKeyspaceName.Load()
		if value != nil {
			keyspaceName = value.(string)
		}
	}

	return keyspaceName == "system" && info.getTableName() == "peers_v2"
}

func isSystemLocal(info queryInfo, currentKeyspaceName *atomic.Value) bool {
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
	frame        *frame.RawFrame // always non nil
	decodedFrame *frame.Frame    // nil until first decode
	queryInfo    queryInfo       // nil until first query inspection
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

func (recv *frameDecodeContext) GetOrInspectQuery() (queryInfo, error) {
	if recv.queryInfo != nil {
		return recv.queryInfo, nil
	}

	decodedFrame, err := recv.GetOrDecodeFrame()
	if err != nil {
		return nil, fmt.Errorf("could not decode frame: %w", err)
	}

	var queryStr string
	switch decodedFrame.Header.OpCode {
	case primitive.OpCodeQuery:
		queryMsg, ok := decodedFrame.Body.Message.(*message.Query)
		if !ok {
			return nil, fmt.Errorf("expected Query but got %v instead", decodedFrame.Body.Message.GetOpCode())
		}
		queryStr = queryMsg.Query
	case primitive.OpCodePrepare:
		prepareMsg, ok := decodedFrame.Body.Message.(*message.Prepare)
		if !ok {
			return nil, fmt.Errorf("expected Prepare but got %v instead", decodedFrame.Body.Message.GetOpCode())
		}
		queryStr = prepareMsg.Query
	default:
		return nil, fmt.Errorf("expected Query or Prepare opcode but got %v instead", decodedFrame.Header.OpCode)
	}

	recv.queryInfo = inspectCqlQuery(queryStr)
	return recv.queryInfo, nil
}