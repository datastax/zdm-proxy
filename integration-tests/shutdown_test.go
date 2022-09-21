package integration_tests

import (
	"context"
	"errors"
	"fmt"
	client2 "github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/client"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/rs/zerolog"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestShutdownInFlightRequests(t *testing.T) {
	testDef := []struct {
		name                  string
		dualReadsEnabled      bool
		asyncReadsOnSecondary bool
	}{
		{
			name:                  "No dual reads",
			dualReadsEnabled:      false,
			asyncReadsOnSecondary: false,
		},
		{
			name:                  "Async Reads on Secondary",
			dualReadsEnabled:      true,
			asyncReadsOnSecondary: true,
		},
	}
	for _, test := range testDef {
		t.Run(test.name, func(t *testing.T) {
			testSetup, err := setup.NewSimulacronTestSetupWithSession(t, false, false)
			defer testSetup.Cleanup()

			config := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
			config.RequestTimeoutMs = 30000
			config.DualReadsEnabled = test.dualReadsEnabled
			config.AsyncReadsOnSecondary = test.asyncReadsOnSecondary
			proxy, err := setup.NewProxyInstanceWithConfig(config)
			require.Nil(t, err)
			shutdownProxyTriggered := false
			defer func() {
				if !shutdownProxyTriggered {
					proxy.Shutdown()
				}
			}()

			cqlClient := client2.NewCqlClient("127.0.0.1:14002", nil)
			cqlConn, err := cqlClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 0)
			if err != nil {
				t.Fatalf("could not connect: %v", err)
			}
			defer cqlConn.Close()

			testSetup.Origin.Prime(
				simulacron.WhenQuery("SELECT * FROM test1", simulacron.NewWhenQueryOptions()).
					ThenSuccess().WithDelay(1 * time.Second))
			testSetup.Origin.Prime(
				simulacron.WhenQuery("SELECT * FROM test2", simulacron.NewWhenQueryOptions()).
					ThenSuccess().WithDelay(3 * time.Second))

			queryMsg1 := &message.Query{
				Query:   "SELECT * FROM test1",
				Options: nil,
			}

			queryMsg2 := &message.Query{
				Query:   "SELECT * FROM test2",
				Options: nil,
			}

			beginTimestamp := time.Now()

			reqFrame := frame.NewFrame(primitive.ProtocolVersion4, 2, queryMsg1)
			inflightRequest, err := cqlConn.Send(reqFrame)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			reqFrame2 := frame.NewFrame(primitive.ProtocolVersion4, 3, queryMsg2)
			inflightRequest2, err := cqlConn.Send(reqFrame2)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			time.Sleep(1 * time.Second)

			shutdownComplete := make(chan bool)
			go func() {
				proxy.Shutdown()
				close(shutdownComplete)
			}()
			shutdownProxyTriggered = true

			select {
			case rsp := <-inflightRequest.Incoming():
				require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)
			case <-time.After(10 * time.Second):
				t.Fatalf("test timed out after 10 seconds")
			}

			// 0.5 second instead of 1 just in case there is a time precision issue
			require.GreaterOrEqual(t, time.Now().Sub(beginTimestamp).Nanoseconds(), (500 * time.Millisecond).Nanoseconds())

			select {
			case <-shutdownComplete:
				t.Fatalf("unexpected shutdown complete before 2nd request is done")
			default:
			}

			reqFrame3 := frame.NewFrame(primitive.ProtocolVersion4, 4, queryMsg1)
			inflightRequest3, err := cqlConn.Send(reqFrame3)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			select {
			case rsp := <-inflightRequest3.Incoming():
				require.Equal(t, primitive.OpCodeError, rsp.Header.OpCode)
				_, ok := rsp.Body.Message.(*message.Overloaded)
				require.True(t, ok)
			case <-time.After(15 * time.Second):
				t.Fatalf("test timed out after 15 seconds")
			}

			select {
			case rsp := <-inflightRequest2.Incoming():
				require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)
			case <-time.After(15 * time.Second):
				t.Fatalf("test timed out after 15 seconds")
			}

			// 2 seconds instead of 3 just in case there is a time precision issue
			require.GreaterOrEqual(t, time.Now().Sub(beginTimestamp).Nanoseconds(), (2 * time.Second).Nanoseconds())

			select {
			case <-shutdownComplete:
			case <-time.After(10 * time.Second):
				t.Fatalf("test timed out")
			}
		})
	}
}

// Test for a race condition that causes a panic on proxy shutdown
func TestStressShutdown(t *testing.T) {
	//t.Skip("test is currently failing due to ZDM-378") //TODO ZDM-378
	testDef := []struct {
		name                  string
		dualReadsEnabled      bool
		asyncReadsOnSecondary bool
	}{
		{
			name:                  "No dual reads",
			dualReadsEnabled:      false,
			asyncReadsOnSecondary: false,
		},
		{
			name:                  "Async Reads on Secondary",
			dualReadsEnabled:      true,
			asyncReadsOnSecondary: true,
		},
	}
	for _, test := range testDef {
		t.Run(test.name, func(t *testing.T) {
			f := func() bool {
				testSetup, err := setup.NewSimulacronTestSetupWithSession(t, false, false)
				require.Nil(t, err)
				defer testSetup.Cleanup()
				cfg := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
				cfg.AsyncReadsOnSecondary = test.asyncReadsOnSecondary
				cfg.DualReadsEnabled = test.dualReadsEnabled
				cfg.RequestTimeoutMs = 5000
				proxy, err := setup.NewProxyInstanceWithConfig(cfg)
				require.Nil(t, err)

				shutdownProxyTriggeredMutex := sync.Mutex{}
				shutdownProxyTriggered := atomic.Value{}
				shutdownProxyTriggered.Store(false)

				globalCtx, globalCancelFn := context.WithCancel(context.Background())
				defer globalCancelFn()

				globalWg := &sync.WaitGroup{}
				defer globalWg.Wait()

				// set up a test timeout goroutine so if something goes wrong and the test takes too long then it gets canceled
				globalWg.Add(1)
				go func() {
					defer globalWg.Done()
					select {
					case <-globalCtx.Done():
					case <-time.After(60*time.Second):
					}

					shutdownProxyTriggeredMutex.Lock()
					if !shutdownProxyTriggered.Load().(bool) {
						shutdownProxyTriggered.Store(true)
						shutdownProxyTriggeredMutex.Unlock()
						proxy.Shutdown()
						globalCancelFn()
						return
					}
					shutdownProxyTriggeredMutex.Unlock()
				}()

				// reduce log verbosity because this test generates a lot of log messages
				oldLevel := log.GetLevel()
				oldZeroLogLevel := zerolog.GlobalLevel()
				log.SetLevel(log.WarnLevel)
				defer log.SetLevel(oldLevel)
				zerolog.SetGlobalLevel(zerolog.WarnLevel)
				defer zerolog.SetGlobalLevel(oldZeroLogLevel)

				cqlConn, err := client.NewTestClientWithRequestTimeout(context.Background(), "127.0.0.1:14002", 10 * time.Second)
				require.Nil(t, err)
				defer cqlConn.Shutdown()

				err = cqlConn.PerformDefaultHandshake(context.Background(), primitive.ProtocolVersion4, false)
				require.Nil(t, err)

				// create a channel that will receive errors from goroutines that are sending requests,
				// the main test goroutine will read from this channel and cause the test to fail if any error is read
				errChan := make(chan error, 1000)

				// create a separate wait group so the test can wait only for the "worker" goroutines
				// (those who will send requests as part of the test) to terminate
				requestsWg := &sync.WaitGroup{}

				// make sure the global test wg is only done when the "worker" goroutines are done
				globalWg.Add(1)
				go func() {
					defer globalWg.Done()
					requestsWg.Wait()
				}()

				// start goroutines that continuously (until proxy shutdown) open connections, send heartbeats and perform handshake
				// (goal is to test if proxy panics (race conditions) when shutting down and receiving requests simultaneously
				for j := 0; j < runtime.GOMAXPROCS(0); j++ {
					requestsWg.Add(1)
					go func() {
						defer requestsWg.Done()
						for {
							id := rand.Int()
							connTimeoutCtx, connTimeoutCancelFn := context.WithTimeout(globalCtx, 5*time.Second)
							tempCqlConn, err := client.NewTestClientWithRequestTimeout(connTimeoutCtx, "127.0.0.1:14002", 10 *time.Second)
							connTimeoutCancelFn() // avoid context leak

							// create new waitgroup dedicated for this "iteration" so the next iteration starts only
							// when all the goroutines of the previous iteration have terminated
							optionsWg := &sync.WaitGroup{}
							if err == nil {
								defaultHandshakeDoneCh := make(chan bool, 1)

								// start a bunch of sub goroutines that use the newly created connection to send OPTIONS
								// requests (heartbeats) while the current goroutine attempts to perform a handshake after a random delay
								for x := 0; x < runtime.GOMAXPROCS(0)*8; x++ {
									optionsWg.Add(1)
									go func() {
										defer optionsWg.Done()
										for {
											select {
											// make sure these sub goroutines terminate when the handshake has been successful (initiated by the parent goroutine)
											case <-defaultHandshakeDoneCh:
												return
											default:
												rspFrame, _, err := tempCqlConn.SendMessage(context.Background(), primitive.ProtocolVersion4, &message.Options{})
												if err != nil {
													if !shutdownProxyTriggered.Load().(bool) {
														errChan <- fmt.Errorf("[%v] unexpected error in heartbeat: %w", id, err)
													}
													return
												}
												switch resultMsg := rspFrame.Body.Message.(type) {
												case *message.Overloaded:
													if !shutdownProxyTriggered.Load().(bool) {
														errChan <- fmt.Errorf("[%v] unexpected overloaded in heartbeat: %v", id, resultMsg)
														return
													}
												case *message.Supported:
												default:
													errChan <- fmt.Errorf("[%v] unexpected result in heartbeat: %v", id, resultMsg)
													return
												}
											}
										}
									}()
								}
								r := rand.Intn(500) + 100
								select {
								case <-time.After(time.Duration(r) * time.Millisecond):
								case <-globalCtx.Done():
								}
								err = tempCqlConn.PerformDefaultHandshake(context.Background(), primitive.ProtocolVersion4, false)
								defaultHandshakeDoneCh <- true
								optionsWg.Wait()
								_ = tempCqlConn.Shutdown()
							}

							if err != nil {
								if !shutdownProxyTriggered.Load().(bool) {
									errChan <- fmt.Errorf("error connecting in handshake: %w", err)
								}
								return
							}
						}
					}()
				}

				// start a single goroutine that continuously sends a query (test if an active connection gets an Overloaded result on proxy shutdown)
				requestsWg.Add(1)
				go func() {
					defer requestsWg.Done()
					for {
						queryMsg := &message.Query{
							Query:   "SELECT * FROM system.local",
							Options: &message.QueryOptions{Consistency: primitive.ConsistencyLevelLocalOne},
						}
						rsp, _, err := cqlConn.SendMessage(context.Background(), primitive.ProtocolVersion4, queryMsg)

						if err != nil {
							if !shutdownProxyTriggered.Load().(bool) {
								errChan <- fmt.Errorf("expected error on query send %w", err)
							}
							return
						}

						if rsp.Header.OpCode != primitive.OpCodeError && rsp.Header.OpCode != primitive.OpCodeResult {
							errChan <- fmt.Errorf("expected error or result actual %v", rsp.Header.OpCode)
							return
						}

						if rsp.Header.OpCode == primitive.OpCodeError {
							if !shutdownProxyTriggered.Load().(bool) {
								errChan <- fmt.Errorf("unexpected error result when proxy shutdown wasn't triggered: %v", rsp.Body.Message)
								return
							}
							_, ok := rsp.Body.Message.(*message.Overloaded)
							if !ok {
								errChan <- fmt.Errorf("expected %v actual %v", "*message.Overloaded", rsp.Body.Message)
								return
							}
						}
					}
				}()

				testDoneCh := make(chan bool)
				globalWg.Add(1)
				go func() {
					defer globalWg.Done()
					defer close(testDoneCh)
					defer globalCancelFn()

					time.Sleep(12000 * time.Millisecond)

					shutdownProxyTriggeredMutex.Lock()
					if shutdownProxyTriggered.Load().(bool) {
						shutdownProxyTriggeredMutex.Unlock()
						t.Errorf("test timed out")
						errChan <- errors.New("test timed out")
						return
					}
					shutdownProxyTriggered.Store(true)
					shutdownProxyTriggeredMutex.Unlock()
					now := time.Now()
					proxy.Shutdown()
					afterShutdownNow := time.Now()
					if afterShutdownNow.Sub(now) > (5 * time.Second) {
						t.Errorf("proxy shutdown took too long (%v milliseconds, threshold is 5 seconds)", afterShutdownNow.Sub(now).Milliseconds())
						errChan <- fmt.Errorf("proxy shutdown took too long (%v milliseconds, threshold is 5 seconds)", afterShutdownNow.Sub(now).Milliseconds())
					}

					// create a channel where a read returns when all the "worker" goroutines of this test have terminated
					requestsDoneCh := make(chan bool)
					go func() {
						requestsWg.Wait()
						close(requestsDoneCh)
					}()

					// wait until all "worker" goroutines have terminated (with a timeout in case something went wrong)
					select {
					case <-requestsDoneCh:
						globalCancelFn()
					case <-time.After(15 * time.Second):
						t.Errorf("timed out waiting for goroutines to finish normally")
						errChan <- errors.New("timed out waiting for goroutines to finish normally")
						globalCancelFn()
						<-requestsDoneCh // after triggering a cancellation of the global context, the worker goroutines should terminate
					}
				}()

				var lastErr error
				done := false
				for !done {
					select {
					case <-testDoneCh:
						done = true
					case e, ok := <-errChan:
						if !ok {
							done = true
							break
						}
						lastErr = e
						t.Logf("error found: %v", e)
					}
				}

				if lastErr != nil {
					t.Errorf("an error (or more) occured. last error: %v", lastErr)
					return false
				} else {
					return true
				}
			}

			for testCount := 1; testCount <= 20; testCount++ {
				if !f() {
					return
				}
			}
		})
	}
}
