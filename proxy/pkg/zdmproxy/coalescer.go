package zdmproxy

import (
	"bytes"
	"context"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
)

const (
	initialBufferSize = 1024
)

// Coalesces writes using a write buffer
type writeCoalescer struct {
	connection net.Conn
	conf       *config.Config

	clientHandlerWaitGroup *sync.WaitGroup
	shutdownContext        context.Context
	cancelFunc             context.CancelFunc

	writeQueue chan *frame.RawFrame

	logPrefix string

	waitGroup *sync.WaitGroup

	writeBufferSizeBytes int

	scheduler *Scheduler
}

func NewWriteCoalescer(
	conf *config.Config,
	conn net.Conn,
	clientHandlerWaitGroup *sync.WaitGroup,
	shutdownContext context.Context,
	clientHandlerCancelFunc context.CancelFunc,
	logPrefix string,
	isRequest bool,
	isAsync bool,
	scheduler *Scheduler) *writeCoalescer {

	writeQueueSizeFrames := conf.RequestWriteQueueSizeFrames
	if !isRequest {
		writeQueueSizeFrames = conf.ResponseWriteQueueSizeFrames
	}
	if isAsync {
		writeQueueSizeFrames = conf.AsyncConnectorWriteQueueSizeFrames
	}

	writeBufferSizeBytes := conf.RequestWriteBufferSizeBytes
	if !isRequest {
		writeBufferSizeBytes = conf.ResponseWriteBufferSizeBytes
	}
	if isAsync {
		writeBufferSizeBytes = conf.AsyncConnectorWriteBufferSizeBytes
	}
	return &writeCoalescer{
		connection:             conn,
		conf:                   conf,
		clientHandlerWaitGroup: clientHandlerWaitGroup,
		shutdownContext:        shutdownContext,
		cancelFunc:             clientHandlerCancelFunc,
		writeQueue:             make(chan *frame.RawFrame, writeQueueSizeFrames),
		logPrefix:              logPrefix,
		waitGroup:              &sync.WaitGroup{},
		writeBufferSizeBytes:   writeBufferSizeBytes,
		scheduler:              scheduler,
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

		draining := false
		bufferedWriter := bytes.NewBuffer(make([]byte, 0, initialBufferSize))
		wg := &sync.WaitGroup{}
		defer wg.Wait()

		for {
			var resultOk bool
			var result *coalescerIterationResult

			firstFrame, firstFrameOk := <-recv.writeQueue
			if !firstFrameOk {
				break
			}

			resultChannel := make(chan *coalescerIterationResult, 1)
			tempDraining := draining
			tempBuffer := bufferedWriter
			wg.Add(1)
			recv.scheduler.Schedule(func() {
				defer wg.Done()
				firstFrameRead := false
				for {
					var f *frame.RawFrame
					var ok bool
					if firstFrameRead {
						select {
						case f, ok = <-recv.writeQueue:
						default:
							ok = false
						}

						if !ok {
							t := &coalescerIterationResult{
								buffer:   tempBuffer,
								draining: tempDraining,
							}
							resultChannel <- t
							close(resultChannel)
							return
						}

						if tempDraining {
							// continue draining the write queue without writing on connection until it is closed
							log.Tracef("[%v] Discarding frame from write queue because shutdown was requested: %v", recv.logPrefix, f.Header)
							continue
						}
					} else {
						firstFrameRead = true
						f = firstFrame
						ok = true
					}

					log.Tracef("[%v] Writing %v on %v", recv.logPrefix, f.Header, connectionAddr)
					err := writeRawFrame(tempBuffer, connectionAddr, recv.shutdownContext, f)
					if err != nil {
						tempDraining = true
						handleConnectionError(err, recv.shutdownContext, recv.cancelFunc, recv.logPrefix, "writing", connectionAddr)
					} else {
						if tempBuffer.Len() >= recv.writeBufferSizeBytes {
							t := &coalescerIterationResult{
								buffer:   tempBuffer,
								draining: tempDraining,
							}
							resultChannel <- t
							close(resultChannel)
							return
						}
					}
				}
			})

			result, resultOk = <-resultChannel
			if !resultOk {
				break
			}

			draining = result.draining
			bufferedWriter = result.buffer
			if bufferedWriter.Len() > 0 && !draining {
				_, err := recv.connection.Write(bufferedWriter.Bytes())
				bufferedWriter.Reset()
				if err != nil {
					handleConnectionError(err, recv.shutdownContext, recv.cancelFunc, recv.logPrefix, "writing", connectionAddr)
					draining = true
				}
			}
		}
	}()
}

func (recv *writeCoalescer) Enqueue(frame *frame.RawFrame) {
	log.Tracef("[%v] Sending %v to write queue on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
	recv.writeQueue <- frame
	log.Tracef("[%v] Sent %v to write queue on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
}

func (recv *writeCoalescer) EnqueueAsync(frame *frame.RawFrame) bool {
	log.Tracef("[%v] Sending %v to write queue on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
	select {
	case recv.writeQueue <- frame:
		log.Tracef("[%v] Sent %v to write queue on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
		return true
	default:
		log.Debugf("[%v] Discarded %v because write queue is full on %v", recv.logPrefix, frame.Header, recv.connection.RemoteAddr())
		return false
	}
}

func (recv *writeCoalescer) Close() {
	close(recv.writeQueue)
	recv.waitGroup.Wait()
}

type coalescerIterationResult struct {
	buffer   *bytes.Buffer
	draining bool
}
