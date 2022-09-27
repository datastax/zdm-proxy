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
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/rs/zerolog"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestShutdownInFlightRequests(t *testing.T) {
	testDef := []struct {
		name     string
		readMode string
	}{
		{
			name:     "No dual reads",
			readMode: config.ReadModePrimaryOnly,
		},
		{
			name:     "Async Reads on Secondary",
			readMode: config.ReadModeDualAsyncOnSecondary,
		},
	}
	for _, test := range testDef {
		t.Run(test.name, func(t *testing.T) {
			testSetup, err := setup.NewSimulacronTestSetupWithSession(false, false)
			defer testSetup.Cleanup()

			config := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
			config.ProxyRequestTimeoutMs = 30000
			config.ReadMode = test.readMode
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
	testDef := []struct {
		name     string
		readMode string
	}{
		{
			name:     "No dual reads",
			readMode: config.ReadModePrimaryOnly,
		},
		{
			name:     "Async Reads on Secondary",
			readMode: config.ReadModeDualAsyncOnSecondary,
		},
	}
	for _, test := range testDef {
		t.Run(test.name, func(t *testing.T) {
			f := func() {
				testSetup, err := setup.NewSimulacronTestSetupWithSession(false, false)
				require.Nil(t, err)
				defer testSetup.Cleanup()
				cfg := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
				cfg.ReadMode = test.readMode
				proxy, err := setup.NewProxyInstanceWithConfig(cfg)
				require.Nil(t, err)
				shutdownProxyTriggered := atomic.Value{}
				shutdownProxyTriggered.Store(false)
				defer func() {
					if !shutdownProxyTriggered.Load().(bool) {
						proxy.Shutdown()
					}
				}()

				cqlConn, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
				require.Nil(t, err)
				defer cqlConn.Shutdown()

				oldLevel := log.GetLevel()
				oldZeroLogLevel := zerolog.GlobalLevel()
				log.SetLevel(log.WarnLevel)
				defer log.SetLevel(oldLevel)
				zerolog.SetGlobalLevel(zerolog.WarnLevel)
				defer zerolog.SetGlobalLevel(oldZeroLogLevel)

				wg := &sync.WaitGroup{}
				defer wg.Wait()

				ctx, cancelFn := context.WithCancel(context.Background())
				defer cancelFn()

				err = cqlConn.PerformDefaultHandshake(ctx, primitive.ProtocolVersion4, false)
				require.Nil(t, err)

				goCtx, goCancel := context.WithCancel(context.Background())
				goWg := &sync.WaitGroup{}
				errChan := make(chan error, 1000)
				requestsWg := &sync.WaitGroup{}
				for j := 0; j < 20; j++ {
					wg.Add(1)
					goWg.Add(1)
					requestsWg.Add(1)
					go func() {
						defer wg.Done()
						defer requestsWg.Done()
						newCtx, cancelFn := context.WithTimeout(ctx, 5000*time.Millisecond)
						tempCqlConn, err := client.NewTestClient(newCtx, "127.0.0.1:14002")
						optionsWg := &sync.WaitGroup{}
						if err == nil {
							for x := 0; x < 50; x++ {
								optionsWg.Add(1)
								go func() {
									defer optionsWg.Done()
									for newCtx.Err() == nil {
										_, _, _ = tempCqlConn.SendMessage(newCtx, primitive.ProtocolVersion4, &message.Options{})
									}
								}()
							}
							goWg.Done()
							<-goCtx.Done()
							r := rand.Intn(20) + 85
							time.Sleep(time.Duration(r) * time.Millisecond)
							err = tempCqlConn.PerformDefaultHandshake(newCtx, primitive.ProtocolVersion4, false)
							cancelFn()
						}
						optionsWg.Wait()

						if tempCqlConn != nil {
							tempCqlConn.Shutdown()
						}

						if err != nil && ctx.Err() == nil {
							if !shutdownProxyTriggered.Load().(bool) &&
								!errors.Is(err, context.DeadlineExceeded) {
								errChan <- err
								return
							}
						}
					}()
				}

				for jj := 0; jj < 1; jj++ {
					wg.Add(1)
					requestsWg.Add(1)
					j := jj
					go func() {
						defer requestsWg.Done()
						defer wg.Done()
						for i := 0; ctx.Err() == nil; i++ {
							queryMsg := &message.Query{
								Query:   "SELECT * FROM system.local",
								Options: &message.QueryOptions{Consistency: primitive.ConsistencyLevelLocalOne},
							}
							newCtx, cancelFn := context.WithTimeout(ctx, 15*time.Second)
							rsp, _, err := cqlConn.SendMessage(newCtx, primitive.ProtocolVersion4, queryMsg)
							cancelFn()

							if err != nil {
								t.Logf("Break fatal on j=%v i=%v", j, i)
								return
							}

							if rsp.Header.OpCode != primitive.OpCodeError && rsp.Header.OpCode != primitive.OpCodeResult {
								errChan <- fmt.Errorf("expected error or result actual %v", rsp.Header.OpCode)
								return
							}

							if rsp.Header.OpCode == primitive.OpCodeError {
								_, ok := rsp.Body.Message.(*message.Overloaded)
								if !ok {
									errChan <- fmt.Errorf("expected %v actual %v", "*message.Overloaded", rsp.Body.Message)
									return
								}
							}
						}
					}()
				}

				requestsDoneCh := make(chan bool)
				wg.Add(1)
				go func() {
					defer wg.Done()
					requestsWg.Wait()
					close(requestsDoneCh)
				}()

				wg.Add(1)
				go func() {
					defer wg.Done()
					defer close(errChan)

					wg.Add(1)
					go func() {
						defer wg.Done()
						goWg.Wait()
						shutdownProxyTriggered.Store(true)
						goCancel()
						time.Sleep(100 * time.Millisecond)
						proxy.Shutdown()
						requestsWg.Wait()
						cancelFn()
					}()
					select {
					case <-ctx.Done():
					case <-time.After(15 * time.Second):
						errChan <- errors.New("timed out waiting for requests to end")
					}
				}()

				var lastErr error
				testDone := false
				for !testDone {
					e, ok := <-errChan
					if !ok {
						testDone = true
						break
					}
					lastErr = e
					t.Logf("error found: %v", e)
				}

				if lastErr != nil {
					t.Errorf("an error (or more) occured. last error: %v", lastErr)
				}
			}

			for testCount := 1; testCount <= 20; testCount++ {
				f()
			}
		})
	}
}
