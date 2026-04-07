package httpzdmproxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockToggle struct {
	enabled bool
}

func (m *mockToggle) SetTargetEnabled(enabled bool) {
	m.enabled = enabled
}

func (m *mockToggle) IsTargetEnabled() bool {
	return m.enabled
}

func TestTargetHandler_GetStatus_Enabled(t *testing.T) {
	toggle := &mockToggle{enabled: true}
	handler := TargetHandler(toggle)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/target", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp targetStatusResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.True(t, resp.Enabled)
}

func TestTargetHandler_GetStatus_Disabled(t *testing.T) {
	toggle := &mockToggle{enabled: false}
	handler := TargetHandler(toggle)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/target", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp targetStatusResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.False(t, resp.Enabled)
}

func TestTargetHandler_Disable(t *testing.T) {
	toggle := &mockToggle{enabled: true}
	handler := TargetHandler(toggle)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/target/disable", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp targetStatusResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.False(t, resp.Enabled)
	require.False(t, toggle.enabled)
}

func TestTargetHandler_Enable(t *testing.T) {
	toggle := &mockToggle{enabled: false}
	handler := TargetHandler(toggle)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/target/enable", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp targetStatusResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.True(t, resp.Enabled)
	require.True(t, toggle.enabled)
}

func TestTargetHandler_DisableThenEnable(t *testing.T) {
	toggle := &mockToggle{enabled: true}
	handler := TargetHandler(toggle)

	// Disable
	req := httptest.NewRequest(http.MethodPost, "/api/v1/target/disable", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	require.False(t, toggle.enabled)

	// Enable
	req = httptest.NewRequest(http.MethodPost, "/api/v1/target/enable", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	require.True(t, toggle.enabled)
}

func TestTargetHandler_MethodNotAllowed(t *testing.T) {
	toggle := &mockToggle{enabled: true}
	handler := TargetHandler(toggle)

	// POST to status endpoint
	req := httptest.NewRequest(http.MethodPost, "/api/v1/target", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)

	// GET to enable endpoint
	req = httptest.NewRequest(http.MethodGet, "/api/v1/target/enable", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)

	// GET to disable endpoint
	req = httptest.NewRequest(http.MethodGet, "/api/v1/target/disable", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func TestTargetHandler_ProxyNotReady(t *testing.T) {
	handler := TargetHandler(nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/target", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusServiceUnavailable, rr.Code)
	var resp targetStatusResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Equal(t, "proxy not ready", resp.Message)
}
