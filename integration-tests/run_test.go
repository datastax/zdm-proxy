package integration_tests

import (
	"context"
	"errors"
	"fmt"
	"github.com/datastax/zdm-proxy/integration-tests/client"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/cloudgateproxy"
	"github.com/jpillora/backoff"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRunWithRetries tests that the proxy is able to accept client connections even if the cluster nodes are unavailable
// at startup but they come back online afterwards
func TestRunWithRetries(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSession(false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	err = testSetup.Origin.DisableConnectionListener()
	require.True(t, err == nil, "failed to disable origin connection listener: %v", err)

	err = testSetup.Target.DisableConnectionListener()
	require.True(t, err == nil, "failed to disable target connection listener: %v", err)

	waitGroup := &sync.WaitGroup{}
	ctx, cancelFunc := context.WithCancel(context.Background())

	defer waitGroup.Wait()
	defer cancelFunc()

	b := &backoff.Backoff{
		Factor: 2,
		Jitter: false,
		Min:    100 * time.Millisecond,
		Max:    500 * time.Millisecond,
	}
	proxy := atomic.Value{}
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		p, err := cloudgateproxy.RunWithRetries(
			setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint()), ctx, b)
		if err == nil {
			proxy.Store(&p)
			<-ctx.Done()
			p.Shutdown()
		}
	}()

	expectedFailureFunc := func(errMsg string) (err error, fatal bool) {
		testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
		if err == nil {
			testClient.Shutdown()
			return errors.New(errMsg), false
		}
		if proxy.Load() != nil {
			return errors.New("expected RunWithRetries to not return but it did"), false
		}
		return nil, false
	}

	utils.RequireWithRetries(t,
		func() (error, bool) {
			return expectedFailureFunc("expected client tcp connection failure but proxy accepted connection")
		}, 10, 100)

	err = testSetup.Origin.EnableConnectionListener()
	require.Nil(t, err)

	utils.RequireWithRetries(t,
		func() (error, bool) {
			return expectedFailureFunc("expected client tcp connection failure after origin enable listener but proxy accepted connection")
		}, 10, 100)

	err = testSetup.Target.EnableConnectionListener()
	require.Nil(t, err)

	utils.RequireWithRetries(t, func() (err error, fatal bool) {
		testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
		if err != nil {
			return fmt.Errorf("expected successful connection attempt but proxy refused: %w", err), false
		}
		defer testClient.Shutdown()
		if proxy.Load() == nil {
			return errors.New("expected RunWithRetries to return but it did not"), false
		}
		return nil, false
	}, 10, 100)
}
