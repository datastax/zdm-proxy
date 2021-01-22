package cloudgateproxy

import (
	"bufio"
	"context"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
)

// Coalesces writes using a write buffer
type writeCoalescer struct {
	connection       net.Conn
	conf             *config.Config
	metricsHandler   metrics.IMetricsHandler

	clientHandlerWaitGroup  *sync.WaitGroup
	clientHandlerContext    context.Context
	clientHandlerCancelFunc context.CancelFunc

	writeQueue chan *frame.RawFrame

	logPrefix string

	waitGroup *sync.WaitGroup
}

func NewWriteCoalescer(
	conf *config.Config,
	conn net.Conn,
	handler metrics.IMetricsHandler,
	clientHandlerWaitGroup *sync.WaitGroup,
	clientHandlerContext context.Context,
	clientHandlerCancelFunc context.CancelFunc,
	logPrefix string) *writeCoalescer {

	return &writeCoalescer{
		connection:              conn,
		conf:                    conf,
		metricsHandler:          handler,
		clientHandlerWaitGroup:  clientHandlerWaitGroup,
		clientHandlerContext:    clientHandlerContext,
		clientHandlerCancelFunc: clientHandlerCancelFunc,
		writeQueue:              make(chan *frame.RawFrame, conf.WriteQueueSizeFrames),
		logPrefix:               logPrefix,
		waitGroup:               &sync.WaitGroup{},
	}
}

func (recv *writeCoalescer) RunWriteQueueLoop() {
	connectionAddr := recv.connection.RemoteAddr().String()
	log.Tracef("[%v] WriteQueueLoop starting for %v", recv.logPrefix, connectionAddr)

	recv.clientHandlerWaitGroup.Add(1)
	recv.waitGroup.Add(1)
	go func() {
		defer recv.clientHandlerWaitGroup.Done()
		defer recv.waitGroup.Done()

		bufferedWriter := bufio.NewWriterSize(recv.connection, recv.conf.WriteBufferSizeBytes)
		draining := false

		for {
			var f *frame.RawFrame
			var ok bool

			select {
			case f, ok = <-recv.writeQueue:
				if !ok {
					return
				}
			default:
				if bufferedWriter.Buffered() > 0 && !draining {
					err := bufferedWriter.Flush()
					if err != nil {
						draining = true
						handleConnectionError(err, recv.clientHandlerCancelFunc, recv.logPrefix, "writing", connectionAddr)
					}
					continue
				} else {
					f, ok = <-recv.writeQueue
					if !ok {
						return
					}
				}
			}

			if draining {
				// continue draining the write queue without writing on connection until it is closed
				log.Tracef("[%v] Discarding frame from write queue because shutdown was requested: %v", recv.logPrefix, f.Header)
				continue
			}

			log.Debugf("[%v] Writing %v on %v", recv.logPrefix, f.Header, connectionAddr)
			err := writeRawFrame(bufferedWriter, connectionAddr, recv.clientHandlerContext, f)
			if err != nil {
				draining = true
				handleConnectionError(err, recv.clientHandlerCancelFunc, recv.logPrefix, "writing", connectionAddr)
			}
		}
	}()
}

func (recv *writeCoalescer) Enqueue(frame *frame.RawFrame) {
	log.Tracef("[%v] Sending %v to write queue on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
	recv.writeQueue <- frame
	log.Tracef("[%v] Sent %v to write queue on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
}

func (recv *writeCoalescer) Close() {
	close(recv.writeQueue)
	recv.waitGroup.Wait()
}