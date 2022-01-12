package cloudgateproxy

import (
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type QueryModifier struct {
	timeUuidGenerator TimeUuidGenerator
}

func NewQueryModifier(timeUuidGenerator TimeUuidGenerator) *QueryModifier {
	return &QueryModifier{timeUuidGenerator: timeUuidGenerator}
}

// replaceQueryString modifies the incoming request in certain conditions:
//   * the request is a QUERY or PREPARE
//   * and it contains now() function calls
func (recv *QueryModifier) replaceQueryString(context *frameDecodeContext) (*frameDecodeContext, []*term, error) {
	requiresReplacement, decodedFrame, queryData, err := requiresQueryReplacement(context)
	if err != nil {
		return nil, nil, err
	}
	if !requiresReplacement {
		return context, []*term{}, nil
	}

	requestType := context.GetRawFrame().Header.OpCode.String()

	var newFrame *frame.Frame
	var replacedTerms []*term
	var newQueryData QueryInfo

	switch context.GetRawFrame().Header.OpCode {
	case primitive.OpCodeQuery:
		newFrame, replacedTerms, newQueryData, err = recv.replaceQueryInQueryMessage(decodedFrame, queryData)
	case primitive.OpCodePrepare:
		newFrame, replacedTerms, newQueryData, err = recv.replaceQueryInPrepareMessage(decodedFrame, queryData)
	default:
		err = fmt.Errorf("request requires query replacement but op code (%v) unrecognized, " +
			"this is most likely a bug", requestType)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("could not replace query string in request '%v': %w", requestType, err)
	}

	newRawFrame, err := defaultCodec.ConvertToRawFrame(newFrame)
	if err != nil {
		return nil, nil, fmt.Errorf("could not convert modified frame to raw frame: %w", err)
	}
	return NewInitializedFrameDecodeContext(newRawFrame, newFrame, newQueryData), replacedTerms, nil
}

func (recv *QueryModifier) replaceQueryInQueryMessage(
	decodedFrame *frame.Frame,
	queryData QueryInfo) (*frame.Frame, []*term, QueryInfo, error) {

	timeUUID := recv.timeUuidGenerator.GetTimeUuid()
	newQueryData, replacedTerms := queryData.replaceNowFunctionCallsWithLiteral(timeUUID)
	newFrame := decodedFrame.Clone()
	newQueryMsg, ok := newFrame.Body.Message.(*message.Query)
	if !ok {
		return nil, nil, nil, fmt.Errorf("expected Query in cloned frame but got %v instead", newFrame.Body.Message.GetOpCode())
	}
	newQueryMsg.Query = newQueryData.getQuery()
	return newFrame, replacedTerms, newQueryData, nil
}

func (recv *QueryModifier) replaceQueryInPrepareMessage(decodedFrame *frame.Frame, queryData QueryInfo) (*frame.Frame, []*term, QueryInfo, error) {
	var newQueryData QueryInfo
	var replacedTerms []*term
	if queryData.hasNamedBindMarkers() {
		newQueryData, replacedTerms = queryData.replaceNowFunctionCallsWithNamedBindMarkers()
	} else {
		newQueryData, replacedTerms = queryData.replaceNowFunctionCallsWithPositionalBindMarkers()
	}
	newFrame := decodedFrame.Clone()
	newPrepareMsg, ok := newFrame.Body.Message.(*message.Prepare)
	if !ok {
		return nil, nil, nil, fmt.Errorf("expected Prepare in cloned frame but got %v instead", newFrame.Body.Message.GetOpCode())
	}
	newPrepareMsg.Query = newQueryData.getQuery()
	return newFrame, replacedTerms, newQueryData, nil
}

func requiresQueryReplacement(context *frameDecodeContext) (bool, *frame.Frame, QueryInfo, error) {
	decodedFrame, queryData, err := context.GetOrDecodeAndInspect()
	if err != nil {
		if errors.Is(err, NotInspectableErr) {
			return false, nil, nil, nil
		}
		return false, nil, nil, fmt.Errorf("could not check whether query needs replacement for a '%v' request: %w",
			context.GetRawFrame().Header.OpCode.String(), err)
	} else if !queryData.hasNowFunctionCalls() {
		return false, decodedFrame, queryData, nil
	}
	return true, decodedFrame, queryData, nil
}