package cloudgateproxy

import "github.com/datastax/go-cassandra-native-protocol/frame"

type Response struct {
	responseFrame *frame.RawFrame
	cluster       ClusterType
	requestFrame  *frame.RawFrame
}

func NewResponse(f *frame.RawFrame, cluster ClusterType) *Response {
	return &Response{
		responseFrame: f,
		cluster:       cluster,
		requestFrame:  nil,
	}
}

func NewTimeoutResponse(requestFrame *frame.RawFrame) *Response {
	return &Response{
		responseFrame: nil,
		cluster:       "",
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