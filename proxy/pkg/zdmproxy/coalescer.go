package zdmproxy

import (
	"context"
	"net"
	"sync"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	log "github.com/sirupsen/logrus"

	"github.com/datastax/zdm-proxy/proxy/pkg/config"
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

	codecHelper *connCodecHelper

	isClusterConnector bool
	isAsyncConnector   bool
}

func NewWriteCoalescer(
	conf *config.Config,
	conn net.Conn,
	clientHandlerWaitGroup *sync.WaitGroup,
	shutdownContext context.Context,
	clientHandlerCancelFunc context.CancelFunc,
	logPrefix string,
	isClusterConnector bool,
	isAsyncConnector bool,
	scheduler *Scheduler,
	codecHelper *connCodecHelper) *writeCoalescer {

	writeQueueSizeFrames := conf.RequestWriteQueueSizeFrames
	if !isClusterConnector {
		writeQueueSizeFrames = conf.ResponseWriteQueueSizeFrames
	}
	if isAsyncConnector {
		writeQueueSizeFrames = conf.AsyncConnectorWriteQueueSizeFrames
	}

	writeBufferSizeBytes := conf.RequestWriteBufferSizeBytes
	if !isClusterConnector {
		writeBufferSizeBytes = conf.ResponseWriteBufferSizeBytes
	}
	if isAsyncConnector {
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
		isClusterConnector:     isClusterConnector,
		isAsyncConnector:       isAsyncConnector,
		codecHelper:            codecHelper,
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
		wg := &sync.WaitGroup{}
		defer wg.Wait()

		state := recv.codecHelper.GetState()

		var resultOk bool
		var result coalescerIterationResult
		for {
			var firstFrame *frame.RawFrame
			var firstFrameOk bool
			if result.leftoverFrame != nil {
				firstFrame = result.leftoverFrame
				firstFrameOk = true
			} else {
				firstFrame, firstFrameOk = <-recv.writeQueue
			}
			if !firstFrameOk {
				break
			}

			result = coalescerIterationResult{}
			resultOk = false

			writeBuffer := recv.codecHelper.segWriter.GetWriteBuffer()
			resultChannel := make(chan coalescerIterationResult, 1)
			wg.Add(1)
			recv.scheduler.Schedule(func() {
				defer wg.Done()
				firstFrameRead := false
				state = recv.codecHelper.GetState()
				for {
					var f *frame.RawFrame
					var ok bool
					if firstFrameRead {
						newState := recv.codecHelper.GetState()
						if newState != state {
							// state updated (compression or segments)
							resultChannel <- coalescerIterationResult{}
							close(resultChannel)
							return
						}
						state = newState

						select {
						case f, ok = <-recv.writeQueue:
						default:
							ok = false
						}

						if !ok {
							resultChannel <- coalescerIterationResult{}
							close(resultChannel)
							return
						}

						if draining {
							// continue draining the write queue without writing on connection until it is closed
							log.Tracef("[%v] Discarding frame from write queue because shutdown was requested: %v", recv.logPrefix, f.Header)
							continue
						}
					} else {
						writeBuffer.Reset()
						firstFrameRead = true
						f = firstFrame
						ok = true
					}

					if !state.useSegments {
						log.Tracef("[%v] Writing %v on %v", recv.logPrefix, f.Header, connectionAddr)
						err := writeRawFrame(writeBuffer, connectionAddr, recv.shutdownContext, f)
						if err != nil {
							draining = true
							handleConnectionError(err, recv.shutdownContext, recv.cancelFunc, recv.logPrefix, "writing", connectionAddr)
						} else {
							if !recv.isClusterConnector {
								// this is the write loop of a client connector so this loop is writing responses
								// we need to switch to segments once READY/AUTHENTICATE response is sent (if v5+)

								if (f.Header.OpCode == primitive.OpCodeReady || f.Header.OpCode == primitive.OpCodeAuthenticate) &&
									f.Header.Version.SupportsModernFramingLayout() {
									resultChannel <- coalescerIterationResult{switchToSegments: true}
									close(resultChannel)
									return
								}
							}

							if writeBuffer.Len() >= recv.writeBufferSizeBytes {
								resultChannel <- coalescerIterationResult{}
								close(resultChannel)
								return
							}
						}
					} else {
						log.Tracef("[%v] Writing %v to segment on %v", recv.logPrefix, f.Header, connectionAddr)
						written, err := recv.codecHelper.segWriter.AppendFrameToSegmentPayload(f)
						if err != nil {
							draining = true
							handleConnectionError(err, recv.shutdownContext, recv.cancelFunc, recv.logPrefix, "writing", connectionAddr)
						} else if !written {
							// need to write current payload before moving forward
							resultChannel <- coalescerIterationResult{leftoverFrame: f}
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
			if draining {
				continue
			}

			if result.switchToSegments {
				err := recv.codecHelper.SetState(true) // don't update local state variable yet, so old state is used to write this buffer
				if err != nil {
					handleConnectionError(err, recv.shutdownContext, recv.cancelFunc, recv.logPrefix, "switching to segments", connectionAddr)
					draining = true
				}
			}

			if writeBuffer.Len() > 0 {
				if draining {
					writeBuffer.Reset()
				} else if !state.useSegments {
					_, err := recv.connection.Write(writeBuffer.Bytes())
					if err != nil {
						handleConnectionError(err, recv.shutdownContext, recv.cancelFunc, recv.logPrefix, "writing", connectionAddr)
						draining = true
					}
				} else {
					err := recv.codecHelper.segWriter.WriteSegments(recv.connection, state)
					if err != nil {
						handleConnectionError(err, recv.shutdownContext, recv.cancelFunc, recv.logPrefix, "writing", connectionAddr)
						draining = true
					}
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
	switchToSegments bool
	leftoverFrame    *frame.RawFrame
}
