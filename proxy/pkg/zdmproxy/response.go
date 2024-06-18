package zdmproxy

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

type Response struct {
	responseFrame *frame.RawFrame
	connectorType ClusterConnectorType
	requestFrame  *frame.RawFrame
}

func NewResponse(f *frame.RawFrame, connectorType ClusterConnectorType) *Response {
	return &Response{
		responseFrame: f,
		connectorType: connectorType,
		requestFrame:  nil,
	}
}

func NewTimeoutResponse(requestFrame *frame.RawFrame, async bool) *Response {
	var connectorType ClusterConnectorType
	if async {
		connectorType = ClusterConnectorTypeAsync
	} else {
		connectorType = ClusterConnectorTypeNone
	}
	return &Response{
		responseFrame: nil,
		connectorType: connectorType,
		requestFrame:  requestFrame,
	}
}

func (r *Response) GetStreamId() int16 {
	if r.responseFrame != nil {
		return r.responseFrame.Header.StreamId
	} else {
		return r.requestFrame.Header.StreamId
	}
}

type ResponseError struct {
	Response *frame.Frame
}

func (pre *ResponseError) Error() string {
	return fmt.Sprintf("%v", pre.Response.Body.Message)
}

func (pre *ResponseError) IsProtocolError() bool {
	switch pre.Response.Body.Message.(type) {
	case *message.ProtocolError:
		return true
	default:
		return false
	}
}
