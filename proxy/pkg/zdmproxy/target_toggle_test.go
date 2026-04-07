package zdmproxy

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics/noopmetrics"
)

// ============================================================
// requestContextImpl with effectiveForwardDecision
// ============================================================

func TestRequestContext_EffectiveForwardDecision_OverrideToOrigin(t *testing.T) {
	// Simulate: requestInfo says forwardToBoth, but effective is forwardToOrigin
	// (target disabled). Request should complete when only origin responds.
	reqInfo := NewGenericRequestInfo(forwardToBoth, false, true)
	reqCtx := NewRequestContext(nil, dummyFrame(), reqInfo, forwardToOrigin, time.Now(), nil)

	require.Equal(t, forwardToOrigin, reqCtx.effectiveForwardDecision)
	require.Equal(t, forwardToBoth, reqCtx.requestInfo.GetForwardDecision())

	nodeMetrics := newNoopNodeMetrics(t)

	// Origin responds — should be "done" since effective decision is forwardToOrigin
	finished := reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeOrigin, ClusterConnectorTypeOrigin)
	require.True(t, finished, "request should be done after origin response when effective decision is forwardToOrigin")
}

func TestRequestContext_EffectiveForwardDecision_BothRequired(t *testing.T) {
	// Normal case: effective decision matches requestInfo (forwardToBoth).
	// Request should NOT complete until both respond.
	reqInfo := NewGenericRequestInfo(forwardToBoth, false, true)
	reqCtx := NewRequestContext(nil, dummyFrame(), reqInfo, forwardToBoth, time.Now(), nil)

	nodeMetrics := newNoopNodeMetrics(t)

	// Origin responds — NOT done yet
	finished := reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeOrigin, ClusterConnectorTypeOrigin)
	require.False(t, finished, "request should NOT be done after only origin response when effective decision is forwardToBoth")

	// Target responds — NOW done
	finished = reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeTarget, ClusterConnectorTypeTarget)
	require.True(t, finished, "request should be done after both responses when effective decision is forwardToBoth")
}

func TestRequestContext_EffectiveForwardDecision_TargetOnlyOverriddenToOrigin(t *testing.T) {
	// forwardToTarget overridden to forwardToOrigin (target disabled, read was directed to target).
	reqInfo := NewGenericRequestInfo(forwardToTarget, false, true)
	reqCtx := NewRequestContext(nil, dummyFrame(), reqInfo, forwardToOrigin, time.Now(), nil)

	nodeMetrics := newNoopNodeMetrics(t)

	// Origin responds — should complete
	finished := reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeOrigin, ClusterConnectorTypeOrigin)
	require.True(t, finished, "request should be done after origin response when forwardToTarget overridden to forwardToOrigin")
}

func TestRequestContext_EffectiveForwardDecision_NoOverride(t *testing.T) {
	// forwardToOrigin with no override — should work normally.
	reqInfo := NewGenericRequestInfo(forwardToOrigin, false, true)
	reqCtx := NewRequestContext(nil, dummyFrame(), reqInfo, forwardToOrigin, time.Now(), nil)

	nodeMetrics := newNoopNodeMetrics(t)

	finished := reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeOrigin, ClusterConnectorTypeOrigin)
	require.True(t, finished)
}

func TestRequestContext_EffectiveForwardDecision_OriginOnlyIgnoresTarget(t *testing.T) {
	// When effective is forwardToOrigin, a target response arriving unexpectedly
	// should NOT cause issues — it should be accepted but completion is based on origin.
	reqInfo := NewGenericRequestInfo(forwardToBoth, false, true)
	reqCtx := NewRequestContext(nil, dummyFrame(), reqInfo, forwardToOrigin, time.Now(), nil)

	nodeMetrics := newNoopNodeMetrics(t)

	// Target responds first (unexpected but possible if race)
	finished := reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeTarget, ClusterConnectorTypeTarget)
	require.False(t, finished, "target response alone should not complete the request when effective is forwardToOrigin")

	// Origin responds — now done
	finished = reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeOrigin, ClusterConnectorTypeOrigin)
	require.True(t, finished)
}

// ============================================================
// Timeout tracking with effectiveForwardDecision
// ============================================================

func TestRequestContext_Timeout_OnlyTracksEffectiveDecision(t *testing.T) {
	// requestInfo says forwardToBoth, effective is forwardToOrigin.
	// On timeout, only origin timeout should be tracked (not target).
	reqInfo := NewGenericRequestInfo(forwardToBoth, false, true)
	reqFrame := dummyFrame()
	reqCtx := NewRequestContext(nil, reqFrame, reqInfo, forwardToOrigin, time.Now(), nil)

	nodeMetrics := newNoopNodeMetrics(t)

	finished := reqCtx.SetTimeout(nodeMetrics, reqFrame)
	require.True(t, finished)
	// Verifying no panic — the noop metrics silently accept any calls.
	// The key assertion is that timeout works with the overridden decision.
}

func TestRequestContext_Timeout_ForwardToBoth(t *testing.T) {
	// Normal case — forwardToBoth effective, both origin and target timeouts tracked.
	reqInfo := NewGenericRequestInfo(forwardToBoth, false, true)
	reqFrame := dummyFrame()
	reqCtx := NewRequestContext(nil, reqFrame, reqInfo, forwardToBoth, time.Now(), nil)

	nodeMetrics := newNoopNodeMetrics(t)

	finished := reqCtx.SetTimeout(nodeMetrics, reqFrame)
	require.True(t, finished)
}

func TestRequestContext_Timeout_AlreadyDone(t *testing.T) {
	// After a response completes the request, timeout should return false.
	reqInfo := NewGenericRequestInfo(forwardToOrigin, false, true)
	reqFrame := dummyFrame()
	reqCtx := NewRequestContext(nil, reqFrame, reqInfo, forwardToOrigin, time.Now(), nil)

	nodeMetrics := newNoopNodeMetrics(t)

	reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeOrigin, ClusterConnectorTypeOrigin)

	finished := reqCtx.SetTimeout(nodeMetrics, reqFrame)
	require.False(t, finished, "timeout should return false when request already completed")
}

// ============================================================
// Cancel
// ============================================================

func TestRequestContext_Cancel_PreventsCompletion(t *testing.T) {
	reqInfo := NewGenericRequestInfo(forwardToOrigin, false, true)
	reqCtx := NewRequestContext(nil, dummyFrame(), reqInfo, forwardToOrigin, time.Now(), nil)

	nodeMetrics := newNoopNodeMetrics(t)

	cancelled := reqCtx.Cancel(nodeMetrics)
	require.True(t, cancelled)

	// Response after cancel should not complete
	finished := reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeOrigin, ClusterConnectorTypeOrigin)
	require.False(t, finished)
}

// ============================================================
// ZdmProxy toggle methods
// ============================================================

func TestZdmProxy_TargetEnabled_DefaultTrue(t *testing.T) {
	targetEnabled := &atomic.Bool{}
	targetEnabled.Store(true)
	proxy := &ZdmProxy{targetEnabled: targetEnabled}

	require.True(t, proxy.IsTargetEnabled())
}

func TestZdmProxy_TargetEnabled_SetDisable(t *testing.T) {
	targetEnabled := &atomic.Bool{}
	targetEnabled.Store(true)
	proxy := &ZdmProxy{targetEnabled: targetEnabled}

	proxy.SetTargetEnabled(false)
	require.False(t, proxy.IsTargetEnabled())
}

func TestZdmProxy_TargetEnabled_SetEnable(t *testing.T) {
	targetEnabled := &atomic.Bool{}
	targetEnabled.Store(false)
	proxy := &ZdmProxy{targetEnabled: targetEnabled}

	proxy.SetTargetEnabled(true)
	require.True(t, proxy.IsTargetEnabled())
}

func TestZdmProxy_TargetEnabled_Idempotent(t *testing.T) {
	targetEnabled := &atomic.Bool{}
	targetEnabled.Store(true)
	proxy := &ZdmProxy{targetEnabled: targetEnabled}

	// Disabling twice should be fine
	proxy.SetTargetEnabled(false)
	proxy.SetTargetEnabled(false)
	require.False(t, proxy.IsTargetEnabled())

	// Enabling twice should be fine
	proxy.SetTargetEnabled(true)
	proxy.SetTargetEnabled(true)
	require.True(t, proxy.IsTargetEnabled())
}

func TestZdmProxy_TargetEnabled_RapidToggle(t *testing.T) {
	targetEnabled := &atomic.Bool{}
	targetEnabled.Store(true)
	proxy := &ZdmProxy{targetEnabled: targetEnabled}

	for i := 0; i < 1000; i++ {
		proxy.SetTargetEnabled(false)
		require.False(t, proxy.IsTargetEnabled())
		proxy.SetTargetEnabled(true)
		require.True(t, proxy.IsTargetEnabled())
	}
}

// ============================================================
// Concurrency: multiple goroutines toggling + reading
// ============================================================

func TestZdmProxy_TargetEnabled_ConcurrentAccess(t *testing.T) {
	targetEnabled := &atomic.Bool{}
	targetEnabled.Store(true)
	proxy := &ZdmProxy{targetEnabled: targetEnabled}

	var wg sync.WaitGroup
	const goroutines = 100
	const iterations = 1000

	// Writers: half enable, half disable
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if id%2 == 0 {
					proxy.SetTargetEnabled(true)
				} else {
					proxy.SetTargetEnabled(false)
				}
			}
		}(i)
	}

	// Readers: just read without crashing
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = proxy.IsTargetEnabled()
			}
		}()
	}

	wg.Wait()
	// No assertion on final value — the point is no races or panics.
}

// ============================================================
// requestContextImpl state transitions with override
// ============================================================

func TestRequestContext_MultipleResponses_IgnoredAfterDone(t *testing.T) {
	// Once done, additional SetResponse calls should return false.
	reqInfo := NewGenericRequestInfo(forwardToOrigin, false, true)
	reqCtx := NewRequestContext(nil, dummyFrame(), reqInfo, forwardToOrigin, time.Now(), nil)

	nodeMetrics := newNoopNodeMetrics(t)

	finished := reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeOrigin, ClusterConnectorTypeOrigin)
	require.True(t, finished)

	// Duplicate response
	finished = reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeOrigin, ClusterConnectorTypeOrigin)
	require.False(t, finished, "duplicate response after completion should be ignored")
}

func TestRequestContext_OverriddenBoth_OriginCompletes_TargetIgnored(t *testing.T) {
	// forwardToBoth overridden to forwardToOrigin.
	// Origin completes the request. A late target response should be ignored.
	reqInfo := NewGenericRequestInfo(forwardToBoth, false, true)
	reqCtx := NewRequestContext(nil, dummyFrame(), reqInfo, forwardToOrigin, time.Now(), nil)

	nodeMetrics := newNoopNodeMetrics(t)

	finished := reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeOrigin, ClusterConnectorTypeOrigin)
	require.True(t, finished)

	// Late target response
	finished = reqCtx.SetResponse(nodeMetrics, dummyFrame(), common.ClusterTypeTarget, ClusterConnectorTypeTarget)
	require.False(t, finished, "late target response after completion should be ignored")
}

// ============================================================
// RequestInfo.GetForwardDecision() is NOT modified
// ============================================================

func TestRequestContext_OriginalRequestInfoUnmodified(t *testing.T) {
	// The requestInfo's forward decision should remain forwardToBoth even when
	// the effective decision is forwardToOrigin. This is critical for per-table
	// write metrics which check requestInfo.GetForwardDecision().
	reqInfo := NewGenericRequestInfo(forwardToBoth, false, true)
	reqInfo.writeTargets = []WriteTarget{{Keyspace: "ks", Table: "tbl"}}

	reqCtx := NewRequestContext(nil, dummyFrame(), reqInfo, forwardToOrigin, time.Now(), nil)

	// effectiveForwardDecision is overridden
	require.Equal(t, forwardToOrigin, reqCtx.effectiveForwardDecision)

	// But requestInfo is untouched
	require.Equal(t, forwardToBoth, reqCtx.GetRequestInfo().GetForwardDecision())
	require.Len(t, reqCtx.GetRequestInfo().GetWriteTargets(), 1)
	require.Equal(t, "ks", reqCtx.GetRequestInfo().GetWriteTargets()[0].Keyspace)
	require.Equal(t, "tbl", reqCtx.GetRequestInfo().GetWriteTargets()[0].Table)
}

// ============================================================
// isWriteStatement helper
// ============================================================

func TestIsWriteStatement_ForwardToBoth(t *testing.T) {
	reqInfo := NewGenericRequestInfo(forwardToBoth, false, true)
	require.True(t, isWriteStatement(reqInfo))
}

func TestIsWriteStatement_ForwardToOrigin(t *testing.T) {
	reqInfo := NewGenericRequestInfo(forwardToOrigin, false, true)
	require.False(t, isWriteStatement(reqInfo))
}

func TestIsWriteStatement_ForwardToTarget(t *testing.T) {
	reqInfo := NewGenericRequestInfo(forwardToTarget, false, true)
	require.False(t, isWriteStatement(reqInfo))
}

// ============================================================
// Helpers
// ============================================================

func dummyFrame() *frame.RawFrame {
	return &frame.RawFrame{
		Header: &frame.Header{
			Version:  primitive.ProtocolVersion4,
			StreamId: 1,
			OpCode:   primitive.OpCodeQuery,
		},
		Body: []byte{},
	}
}

func newNoopNodeMetrics(t *testing.T) *metrics.NodeMetrics {
	t.Helper()
	factory := noopmetrics.NewNoopMetricFactory()
	instance := func() *metrics.NodeMetricsInstance {
		counter, _ := factory.GetOrCreateCounter(metrics.OriginClientTimeouts)
		hist, _ := factory.GetOrCreateHistogram(metrics.OriginRequestDuration, nil)
		gauge, _ := factory.GetOrCreateGauge(metrics.InFlightReadsOrigin)
		return &metrics.NodeMetricsInstance{
			ClientTimeouts:    counter,
			ReadTimeouts:      counter,
			ReadFailures:      counter,
			WriteTimeouts:     counter,
			WriteFailures:     counter,
			UnpreparedErrors:  counter,
			OverloadedErrors:  counter,
			UnavailableErrors: counter,
			OtherErrors:       counter,
			ReadDurations:     hist,
			WriteDurations:    hist,
			OpenConnections:   gauge,
			FailedConnections: counter,
			InFlightRequests:  gauge,
			UsedStreamIds:     gauge,
		}
	}
	return &metrics.NodeMetrics{
		OriginMetrics: instance(),
		TargetMetrics: instance(),
		AsyncMetrics:  instance(),
	}
}
