package integration_tests

import (
	"context"
	"errors"
	"fmt"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSimultaneousConnections(t *testing.T) {
	if !env.RunCcmTests {
		t.Skip("Test requires CCM, set RUN_CCMTESTS env variable to TRUE")
	}
	ccmSetup, err := setup.NewTemporaryCcmTestSetup(false, false)
	require.Nil(t, err)
	defer ccmSetup.Cleanup()
	err = ccmSetup.Origin.UpdateConf("authenticator: PasswordAuthenticator")
	require.Nil(t, err)
	err = ccmSetup.Target.UpdateConf("authenticator: PasswordAuthenticator")
	require.Nil(t, err)

	err = ccmSetup.Start(nil, "-Dcassandra.superuser_setup_delay_ms=0")
	require.Nil(t, err)

	cfg := setup.NewTestConfig(ccmSetup.Origin.GetInitialContactPoint(), ccmSetup.Target.GetInitialContactPoint())
	cfg.ProxyMaxClientConnections = 4000
	cfg.ProxyRequestTimeoutMs = 15000
	cfg.ReadMaxWorkers = 1
	cfg.WriteMaxWorkers = 1
	cfg.RequestResponseMaxWorkers = 1 // set schedulers to 1 to force a deadlock if such deadlock is possible
	testProxy, err := setup.NewProxyInstanceWithConfig(cfg)
	require.Nil(t, err)
	shutdown := int32(0)
	shutdownFunc := func() {
		if atomic.CompareAndSwapInt32(&shutdown, 0, 1) {
			testProxy.Shutdown()
		}
	}
	defer shutdownFunc()

	wg := &sync.WaitGroup{}
	requestWg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		requestWg.Wait()
	}()
	defer wg.Wait()
	testCtx, testCancelFn := context.WithCancel(context.Background())
	defer testCancelFn()
	errChan := make(chan error, 10000)

	oldLevel := log.GetLevel()
	oldZeroLogLevel := zerolog.GlobalLevel()
	log.SetLevel(log.InfoLevel)
	defer log.SetLevel(oldLevel)
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	defer zerolog.SetGlobalLevel(oldZeroLogLevel)

	parallelSessionGoroutines := 20
	numberOfSessionsPerGoroutine := 1

	fatalErr := errors.New("fatal err")
	spawnGoroutinesWg := &sync.WaitGroup{}
	for i := 0; i < parallelSessionGoroutines; i++ {
		spawnGoroutinesWg.Add(1)
		go func() {
			defer spawnGoroutinesWg.Done()
			for i := 0; i < numberOfSessionsPerGoroutine; i++ {
				goCqlCluster := gocql.NewCluster("localhost")
				goCqlCluster.Port = 14002
				goCqlCluster.ProtoVersion = 4
				goCqlCluster.Authenticator = gocql.PasswordAuthenticator{
					Username: "cassandra",
					Password: "cassandra",
				}
				goCqlCluster.NumConns = 1
				goCqlCluster.ReconnectInterval = 100 * time.Millisecond
				goCqlCluster.Timeout = 10 * time.Second
				goCqlCluster.ConnectTimeout = 10 * time.Second
				goCqlSession, err := goCqlCluster.CreateSession()
				if err != nil {
					errChan <- fmt.Errorf("%w: %v", fatalErr, err.Error())
					return
				}
				defer goCqlSession.Close()
				requestWg.Add(1)
				go func() {
					defer requestWg.Done()
					for testCtx.Err() == nil {
						qCtx, fn := context.WithTimeout(testCtx, 10*time.Second)
						q := goCqlSession.Query("SELECT * FROM system_schema.keyspaces").WithContext(qCtx)
						err := q.Exec()
						fn()
						if errors.Is(err, gocql.ErrSessionClosed) {
							return
						}
						if err != nil {
							errChan <- err
						}
						time.Sleep(200 * time.Millisecond)
					}
				}()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(errChan)
		spawnGoroutinesWg.Wait()
		select {
		case <-time.After(13 * time.Second):
		case <-testCtx.Done():
			return
		}
		t.Logf("triggering proxy shutdown")
		shutdownFunc()

		select {
		case <-time.After(13 * time.Second):
		case <-testCtx.Done():
			return
		}

		t.Logf("restarting proxy")
		newTestProxy, err := setup.NewProxyInstanceWithConfig(cfg)
		if err != nil {
			errChan <- err
			return
		}
		defer newTestProxy.Shutdown()
		select {
		case <-time.After(13 * time.Second):
		case <-testCtx.Done():
			return
		}

		testCancelFn()
		requestWg.Wait()
	}()

	for {
		err, ok := <-errChan
		if !ok {
			return
		}
		if errors.Is(err, fatalErr) {
			assert.Failf(t, "fatal error", "%v", err.Error())
			testCancelFn()
		} else if errors.Is(err, context.DeadlineExceeded) {
			assert.Fail(t, "gocql client timeout hit, deadlock?")
			testCancelFn()
		} else if atomic.LoadInt32(&shutdown) == 0 {
			assert.Failf(t, "error before shutdown, deadlock?", "%v", err.Error())
			testCancelFn()
		} else {
			t.Log(err)
		}
	}
}
