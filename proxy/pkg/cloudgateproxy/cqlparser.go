package cloudgateproxy

import (
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

type UnpreparedExecuteError struct {
	Header     *frame.Header
	Body       *frame.Body
	preparedId []byte
}

func (uee *UnpreparedExecuteError) Error() string {
	return fmt.Sprintf("The preparedID of the statement to be executed (%s) does not exist in the proxy cache", uee.preparedId)
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

func inspectFrame(
	frameContext *frameDecodeContext,
	psCache *PreparedStatementCache,
	mh metrics.IMetricsHandler,
	currentKeyspaceName *atomic.Value,
	forwardReadsToTarget bool) (StatementInfo, error) {

	forwardDecision := forwardToBoth
	f := frameContext.frame

	switch f.Header.OpCode {

	case primitive.OpCodeQuery:
		queryInfo, err := frameContext.GetOrInspectQuery()
		if err != nil {
			return nil, fmt.Errorf("could not inspect QUERY frame: %w", err)
		}
		if queryInfo.getStatementType() == statementTypeSelect {
			if isSystemQuery(queryInfo, currentKeyspaceName) || forwardReadsToTarget {
				log.Debugf("Detected system query: %v with stream id: %v",  queryInfo.getQuery(), f.Header.StreamId)
				forwardDecision = forwardToTarget
			} else {
				forwardDecision = forwardToOrigin
			}
		}
		return NewGenericStatementInfo(forwardDecision), nil

	case primitive.OpCodePrepare:
		queryInfo, err := frameContext.GetOrInspectQuery()
		if err != nil {
			return nil, fmt.Errorf("could not inspect PREPARE frame: %w", err)
		}
		if queryInfo.getStatementType() == statementTypeSelect {
			if isSystemQuery(queryInfo, currentKeyspaceName) || forwardReadsToTarget {
				log.Debugf("Detected system query: %v with stream id: %v",  queryInfo.getQuery(), f.Header.StreamId)
				forwardDecision = forwardToTarget
			} else {
				forwardDecision = forwardToOrigin
			}
		}
		return NewPreparedStatementInfo(forwardDecision), nil

	case primitive.OpCodeExecute:
		decodedFrame, err := frameContext.GetOrDecodeFrame()
		if err != nil {
			return nil, fmt.Errorf("could not decode execute raw frame: %w", err)
		}
		executeMsg, ok := decodedFrame.Body.Message.(*message.Execute)
		if !ok {
			return nil, fmt.Errorf("expected Execute but got %v instead", decodedFrame.Body.Message.GetOpCode())
		}
		log.Debugf("Execute with prepared-id = '%s'", executeMsg.QueryId)
		if stmtInfo, ok := psCache.retrieveStmtInfoFromCache(executeMsg.QueryId); ok {
			// The forward decision was set in the cache when handling the corresponding PREPARE request
			return NewGenericStatementInfo(stmtInfo.forwardDecision), nil
		} else {
			log.Warnf("No cached entry for prepared-id = '%s'", executeMsg.QueryId)
			_ = mh.IncrementCountByOne(metrics.PSCacheMissCount)
			// return meaningful error to caller so it can generate an unprepared response
			return nil, &UnpreparedExecuteError{Header: f.Header, Body: decodedFrame.Body, preparedId: executeMsg.QueryId}
		}

	case primitive.OpCodeStartup, primitive.OpCodeAuthResponse:
		return NewGenericStatementInfo(forwardToOrigin), nil

	default:
		return NewGenericStatementInfo(forwardToBoth), nil
	}
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
