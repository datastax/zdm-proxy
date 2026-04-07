package httpzdmproxy

import (
	"encoding/json"
	"net/http"
)

type TargetToggle interface {
	SetTargetEnabled(enabled bool)
	IsTargetEnabled() bool
}

type targetStatusResponse struct {
	Enabled bool   `json:"enabled"`
	Message string `json:"message,omitempty"`
}

func DefaultTargetHandler() http.Handler {
	return TargetHandler(nil)
}

func TargetHandler(toggle TargetToggle) http.Handler {
	return http.HandlerFunc(func(rsp http.ResponseWriter, req *http.Request) {
		if toggle == nil {
			rsp.Header().Set("Content-Type", "application/json")
			rsp.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(rsp).Encode(targetStatusResponse{
				Enabled: true,
				Message: "proxy not ready",
			})
			return
		}

		rsp.Header().Set("Content-Type", "application/json")

		switch req.URL.Path {
		case "/api/v1/target":
			if req.Method != http.MethodGet {
				http.Error(rsp, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			json.NewEncoder(rsp).Encode(targetStatusResponse{
				Enabled: toggle.IsTargetEnabled(),
			})

		case "/api/v1/target/enable":
			if req.Method != http.MethodPost {
				http.Error(rsp, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			toggle.SetTargetEnabled(true)
			json.NewEncoder(rsp).Encode(targetStatusResponse{
				Enabled: true,
				Message: "Target enabled",
			})

		case "/api/v1/target/disable":
			if req.Method != http.MethodPost {
				http.Error(rsp, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			toggle.SetTargetEnabled(false)
			json.NewEncoder(rsp).Encode(targetStatusResponse{
				Enabled: false,
				Message: "Target disabled",
			})

		default:
			http.NotFound(rsp, req)
		}
	})
}
