package zdmproxy

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/google/uuid"
)

type QueryModifier struct {
	timeUuidGenerator TimeUuidGenerator
	conf              *config.Config
}

func NewQueryModifier(timeUuidGenerator TimeUuidGenerator, conf *config.Config) *QueryModifier {
	return &QueryModifier{timeUuidGenerator: timeUuidGenerator, conf: conf}
}

func (recv *QueryModifier) enrichRequest(currentKeyspace string, context *frameDecodeContext) (*frameDecodeContext, []*statementReplacedTerms, error) {
	replacedTerms := []*statementReplacedTerms{}
	var err error
	reEnc := false

	// replace CQL functions
	if recv.conf.ReplaceCqlFunctions {
		reEnc, context, replacedTerms, err = recv.replaceQueryString(currentKeyspace, context)
	}
	if err != nil {
		return nil, nil, err
	}

	// add request ID for distributed tracing
	if recv.conf.EnableTracing {
		reEnc, err = recv.assignRequestId(context)
	}
	if err != nil {
		return nil, nil, err
	}

	if reEnc {
		newRawFrame, err := defaultCodec.ConvertToRawFrame(context.decodedFrame)
		if err != nil {
			return nil, nil, fmt.Errorf("could not convert modified frame to raw frame: %w", err)
		}
		return NewInitializedFrameDecodeContext(newRawFrame, context.decodedFrame, context.statementsQueryData), replacedTerms, nil
	}

	return context, replacedTerms, err
}

// replaceQueryString modifies the incoming request in certain conditions:
//   - the request is a QUERY or PREPARE
//   - and it contains now() function calls
func (recv *QueryModifier) replaceQueryString(currentKeyspace string, context *frameDecodeContext) (bool, *frameDecodeContext, []*statementReplacedTerms, error) {
	decodedFrame, statementsQueryData, err := context.GetOrDecodeAndInspect(currentKeyspace, recv.timeUuidGenerator)
	if err != nil {
		if errors.Is(err, NotInspectableErr) {
			return false, context, []*statementReplacedTerms{}, nil
		}
		return false, nil, nil, fmt.Errorf("could not check whether query needs replacement for a '%v' request: %w",
			context.GetRawFrame().Header.OpCode.String(), err)
	}

	requestType := context.GetRawFrame().Header.OpCode.String()

	var newFrame *frame.Frame
	var replacedTerms []*statementReplacedTerms
	var newStatementsQueryData []*statementQueryData

	switch context.GetRawFrame().Header.OpCode {
	case primitive.OpCodeBatch:
		newFrame, replacedTerms, newStatementsQueryData, err = recv.replaceQueryInBatchMessage(decodedFrame, statementsQueryData)
	case primitive.OpCodeQuery:
		newFrame, replacedTerms, newStatementsQueryData, err = recv.replaceQueryInQueryMessage(decodedFrame, statementsQueryData)
	case primitive.OpCodePrepare:
		newFrame, replacedTerms, newStatementsQueryData, err = recv.replaceQueryInPrepareMessage(decodedFrame, statementsQueryData)
	default:
		err = fmt.Errorf("request requires query replacement but op code (%v) unrecognized, "+
			"this is most likely a bug", requestType)
	}

	if err != nil {
		return false, nil, nil, fmt.Errorf("could not replace query string in request '%v': %w", requestType, err)
	}

	return true, NewInitializedFrameDecodeContext(context.frame, newFrame, newStatementsQueryData), replacedTerms, nil
}

func (recv *QueryModifier) replaceQueryInBatchMessage(
	decodedFrame *frame.Frame,
	statementsQueryData []*statementQueryData) (*frame.Frame, []*statementReplacedTerms, []*statementQueryData, error) {

	if len(statementsQueryData) == 0 {
		return decodedFrame, []*statementReplacedTerms{}, statementsQueryData, nil
	}

	newStatementsQueryData := make([]*statementQueryData, 0, len(statementsQueryData))
	statementsReplacedTerms := make([]*statementReplacedTerms, 0)
	replacedStatementIndexes := make([]int, 0)

	for idx, stmtQueryData := range statementsQueryData {
		if stmtQueryData.queryData.hasNowFunctionCalls() {
			newQueryData, replacedTerms := stmtQueryData.queryData.replaceNowFunctionCallsWithLiteral()
			newStatementsQueryData = append(
				newStatementsQueryData,
				&statementQueryData{statementIndex: stmtQueryData.statementIndex, queryData: newQueryData})
			statementsReplacedTerms = append(
				statementsReplacedTerms,
				&statementReplacedTerms{statementIndex: stmtQueryData.statementIndex, replacedTerms: replacedTerms})
			replacedStatementIndexes = append(replacedStatementIndexes, idx)
		} else {
			newStatementsQueryData = append(newStatementsQueryData, stmtQueryData)
		}
	}

	if len(replacedStatementIndexes) == 0 {
		return decodedFrame, []*statementReplacedTerms{}, statementsQueryData, nil
	}

	newFrame := decodedFrame.DeepCopy()
	newBatchMsg, ok := newFrame.Body.Message.(*message.Batch)
	if !ok {
		return nil, nil, nil, fmt.Errorf("expected Batch in cloned frame but got %v instead", newFrame.Body.Message.GetOpCode())
	}
	for _, idx := range replacedStatementIndexes {
		newStmtQueryData := newStatementsQueryData[idx]
		if newStmtQueryData.statementIndex >= len(newBatchMsg.Children) {
			return nil, nil, nil, fmt.Errorf("new query data statement index (%v) is greater or equal than "+
				"number of batch child statements (%v)", newStmtQueryData.statementIndex, len(newBatchMsg.Children))
		}
		newBatchMsg.Children[newStmtQueryData.statementIndex].Query = newStmtQueryData.queryData.getQuery()
	}

	return newFrame, statementsReplacedTerms, newStatementsQueryData, nil
}

func (recv *QueryModifier) replaceQueryInQueryMessage(
	decodedFrame *frame.Frame,
	statementsQueryData []*statementQueryData) (*frame.Frame, []*statementReplacedTerms, []*statementQueryData, error) {
	requiresReplacement, stmtQueryData, err := queryOrPrepareRequiresQueryReplacement(statementsQueryData)
	if err != nil {
		return nil, nil, nil, err
	}
	if !requiresReplacement {
		return decodedFrame, []*statementReplacedTerms{}, statementsQueryData, nil
	}
	newQueryData, replacedTerms := stmtQueryData.queryData.replaceNowFunctionCallsWithLiteral()
	newFrame := decodedFrame.DeepCopy()
	newQueryMsg, ok := newFrame.Body.Message.(*message.Query)
	if !ok {
		return nil, nil, nil, fmt.Errorf("expected Query in cloned frame but got %v instead", newFrame.Body.Message.GetOpCode())
	}
	newQueryMsg.Query = newQueryData.getQuery()
	return newFrame, []*statementReplacedTerms{{0, replacedTerms}}, []*statementQueryData{{statementIndex: stmtQueryData.statementIndex, queryData: newQueryData}}, nil
}

func (recv *QueryModifier) replaceQueryInPrepareMessage(
	decodedFrame *frame.Frame,
	statementsQueryData []*statementQueryData) (*frame.Frame, []*statementReplacedTerms, []*statementQueryData, error) {
	requiresReplacement, stmtQueryData, err := queryOrPrepareRequiresQueryReplacement(statementsQueryData)
	if err != nil {
		return nil, nil, nil, err
	}
	if !requiresReplacement {
		return decodedFrame, []*statementReplacedTerms{}, statementsQueryData, nil
	}
	var newQueryData QueryInfo
	var replacedTerms []*term
	if stmtQueryData.queryData.hasNamedBindMarkers() {
		newQueryData, replacedTerms = stmtQueryData.queryData.replaceNowFunctionCallsWithNamedBindMarkers()
	} else {
		newQueryData, replacedTerms = stmtQueryData.queryData.replaceNowFunctionCallsWithPositionalBindMarkers()
	}
	newFrame := decodedFrame.DeepCopy()
	newPrepareMsg, ok := newFrame.Body.Message.(*message.Prepare)
	if !ok {
		return nil, nil, nil, fmt.Errorf("expected Prepare in cloned frame but got %v instead", newFrame.Body.Message.GetOpCode())
	}
	newPrepareMsg.Query = newQueryData.getQuery()
	return newFrame, []*statementReplacedTerms{{0, replacedTerms}}, []*statementQueryData{{statementIndex: stmtQueryData.statementIndex, queryData: newQueryData}}, nil
}

func requiresQueryReplacement(stmtQueryData *statementQueryData) bool {
	return stmtQueryData.queryData.hasNowFunctionCalls()
}

func queryOrPrepareRequiresQueryReplacement(statementsQueryData []*statementQueryData) (bool, *statementQueryData, error) {
	if len(statementsQueryData) != 1 {
		return false, nil, fmt.Errorf("expected single query data object but got %v", len(statementsQueryData))
	}

	return requiresQueryReplacement(statementsQueryData[0]), statementsQueryData[0], nil
}

func (recv *QueryModifier) assignRequestId(context *frameDecodeContext) (bool, error) {
	op := context.frame.Header.OpCode
	if op != primitive.OpCodePrepare && op != primitive.OpCodeExecute && op != primitive.OpCodeQuery && op != primitive.OpCodeBatch {
		return false, nil
	}

	decodedFrame, err := context.GetOrDecodeFrame()
	if err != nil {
		return false, fmt.Errorf("could not decode frame: %w", err)
	}
	customPayload := decodedFrame.Body.CustomPayload
	if customPayload == nil {
		customPayload = make(map[string][]byte)
	}
	if _, ok := customPayload["request-id"]; !ok {
		// generate new request ID
		reqId, err := uuid.New().MarshalBinary()
		if err != nil {
			return false, err
		}
		customPayload["request-id"] = reqId
		context.decodedFrame.SetCustomPayload(customPayload)
		return true, nil
	}
	return false, nil
}
