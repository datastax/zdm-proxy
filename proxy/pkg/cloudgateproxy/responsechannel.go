package cloudgateproxy

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"sync"
)

// Struct that manages creation and closing of channels under a single RWMutex.
// Only one channel is active at a time for each instance of responseChannel.
// This is used to coordinate creation and closing of the response channels associated with each stream id.
// Each stream id is associated with a single responseChannel instance and these instances are reused throughout the
// ClusterConnector's lifetime.
type responseChannel struct {
	channel chan *frame.RawFrame
	rwLock  *sync.RWMutex
}

func NewResponseChannel() *responseChannel {
	return &responseChannel{
		channel: nil,
		rwLock:  &sync.RWMutex{},
	}
}

// Opens the channel. Returns the new read only channel from which the response
// can be read and an error. channel is only nil when err is non nil and vice versa.
func (recv *responseChannel) Open() (channel <-chan *frame.RawFrame, err error) {
	recv.rwLock.Lock()
	defer recv.rwLock.Unlock()

	if recv.channel != nil {
		return nil, fmt.Errorf("could not open response channel because it is already open")
	}

	recv.channel = make(chan *frame.RawFrame, 1)
	return recv.channel, nil
}

// Closes the channel if it is open.
// Returns true if the channel was successfully closed and false if it was not (e.g. channel wasn't open).
func (recv *responseChannel) Close() error {
	recv.rwLock.Lock()
	defer recv.rwLock.Unlock()

	if recv.channel == nil {
		return fmt.Errorf("could not close channel because it wasn't open")
	}

	close(recv.channel)
	recv.channel = nil
	return nil
}

// Writes the response to the response channel.
func (recv *responseChannel) WriteResponse(f *frame.RawFrame) error {
	// use read lock because we are not modifying the channel field itself (or closing it)
	recv.rwLock.RLock()
	defer recv.rwLock.RUnlock()

	if recv.channel == nil {
		return fmt.Errorf("response channel for stream id %d is not open", f.Header.StreamId)
	}

	recv.channel <- f
	return nil
}