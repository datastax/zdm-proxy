package cloudgateproxy

import "github.com/datastax/go-cassandra-native-protocol/frame"

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
