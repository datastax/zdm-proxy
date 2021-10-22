package integration_tests

import (
	"context"
	"github.com/jpillora/backoff"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
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
		p, err := cloudgateproxy.RunWithRetries(setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint()), ctx, b)
		if err == nil {
			proxy.Store(&p)
			<-ctx.Done()
			p.Shutdown()
		}
	}()

	time.Sleep(1 * time.Second)

	testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
	if err == nil {
		testClient.Shutdown()
		t.Fatal("expected client tcp connection failure but proxy accepted connection")
	}
	require.True(t, proxy.Load() == nil, "expected RunWithRetries to not return but it did")

	err = testSetup.Origin.EnableConnectionListener()

	time.Sleep(1 * time.Second)

	testClient, err = client.NewTestClient(context.Background(), "127.0.0.1:14002")
	if err == nil {
		testClient.Shutdown()
		t.Fatal("expected client tcp connection failure after origin enable listener but proxy accepted connection")
	}
	require.True(t, proxy.Load() == nil, "expected RunWithRetries to not return but it did")

	err = testSetup.Target.EnableConnectionListener()

	time.Sleep(1 * time.Second)

	testClient, err = client.NewTestClient(context.Background(), "127.0.0.1:14002")
	require.True(t, err == nil, "expected successful connection attempt but proxy refused")
	defer testClient.Shutdown()
	require.True(t, proxy.Load() != nil, "expected RunWithRetries to return but it did not")
}
