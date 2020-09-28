package cloudgateproxy

import (
	"errors"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
)

/*
  This owns:
    - a response channel to send responses back to the client
    - the actual TCP connection
 */

type ClientConnector struct {

	// connection to the client
	connection 					net.Conn

	// channel on which the ClientConnector sends requests as it receives them from the client
	requestChannel chan *Frame
	// channel on which the ClientConnector listens for responses to send to the client
	responseChannel chan []byte

	lock 						*sync.RWMutex          // TODO do we need a lock here?
	metrics						*metrics.MetricsOld // Global metrics object
}

func NewClientConnector(connection net.Conn,
						requestChannel chan *Frame,
						metrics *metrics.MetricsOld) *ClientConnector {
	return &ClientConnector{
		connection:      connection,
		requestChannel:  requestChannel,
		responseChannel: make(chan []byte),
		lock:            &sync.RWMutex{},
		metrics:         metrics,
	}
}

/**
 *	Starts two listening loops: one for receiving requests from the client, one for the responses that must be sent to the client
 */
func (cc *ClientConnector) run() {
	cc.listenForRequests()
	cc.listenForResponses()
}


func (cc *ClientConnector) listenForRequests() {

	log.Debugf("listenForRequests for client %s", cc.connection.RemoteAddr())

	var err error

	// TODO: goroutine needs to handle the error, not return it
	go func() error {
		for {
			var frame *Frame
			frameHeader := make([]byte, cassHdrLen)
			frame, err = parseFrame(cc.connection, frameHeader, cc.metrics)

			if err != nil {
				if err == io.EOF {
					log.Debugf("in listenForRequests: %s disconnected", cc.connection.RemoteAddr())
				} else {
					log.Debugf("in listenForRequests: error reading frame header: %s", err)
					log.Error(err)
				}
				// TODO: handle some errors without stopping the loop?
				log.Debugf("listenForRequests: returning error %s", err)
				return err
			}

			if frame.Direction != 0 {
				log.Debugf("Unexpected frame direction %d", frame.Direction)
				log.Error(errors.New("unexpected direction: frame not from client to db - skipping frame"))
				continue
			}

			log.Debugf("sending frame on channel ")
			cc.requestChannel <- frame
			log.Debugf("frame sent")
		}
	}()
}

// listens on responseChannel, dequeues any responses and sends them to the client
func (cc *ClientConnector) listenForResponses() error {
	log.Debugf("listenForResponses for client %s", cc.connection.RemoteAddr())

	var err error
	go func() {
		for {
			log.Debugf("Waiting for next response to dispatch to client %s", cc.connection.RemoteAddr())
			// TODO: handle channel closed
			response := <-cc.responseChannel

			log.Debugf("Response with opcode %d (%v) received, dispatching to client %s", response[4], string(*&response), cc.connection.RemoteAddr())
			err = writeToConnection(cc.connection, response)
			log.Debugf("Response with opcode %d dispatched to client %s", response[4], cc.connection.RemoteAddr())
			if err != nil {
				log.Errorf("Error writing response to client connection: %s", err)
				break
			}
		}
	}()
	return err
}
