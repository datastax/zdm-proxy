package health

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/datastax/zdm-proxy/proxy/pkg/cloudgateproxy"
	log "github.com/sirupsen/logrus"
	"net/http"
)

func DefaultReadinessHandler() http.Handler {
	return ReadinessHandler(nil)
}

type StatusReport struct {
	OriginStatus *ControlConnStatus
	TargetStatus *ControlConnStatus
	Status       Status
}

type ControlConnStatus struct {
	Addr                  string
	CurrentFailureCount   int
	FailureCountThreshold int
	Status                Status
}

type Status string

const (
	UP               = Status("UP")
	DOWN             = Status("DOWN")
	STARTUP          = Status("STARTUP")
)

func ReadinessHandler(proxy *cloudgateproxy.CloudgateProxy) http.Handler {
	return http.HandlerFunc(func(rsp http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.NotFound(rsp, req)
			return
		}

		report := PerformHealthCheck(proxy)
		bytes, err := json.Marshal(report)
		if err != nil {
			uid := uuid.New()
			msg := fmt.Sprintf("Internal server error with code %v", uid)
			log.Errorf("Could not perform health check (code: %v): %v", uid, err)

			http.Error(rsp, msg, http.StatusInternalServerError)
			return
		}

		header := rsp.Header()
		header.Set("Content-Type", "application/json")
		if report.Status == UP {
			rsp.WriteHeader(http.StatusOK)
		} else {
			rsp.WriteHeader(http.StatusServiceUnavailable)
		}
		rsp.Write(bytes)
	})
}

func LivenessHandler() http.Handler {
	return http.HandlerFunc(func(rsp http.ResponseWriter, req *http.Request) {
		rsp.WriteHeader(http.StatusOK)
		rsp.Write([]byte("OK"))
	})
}

func PerformHealthCheck(proxy *cloudgateproxy.CloudgateProxy) *StatusReport {
	if proxy == nil {
		return &StatusReport{
			OriginStatus: nil,
			TargetStatus: nil,
			Status:       STARTUP,
		}
	}

	originControlConn := proxy.GetOriginControlConn()
	targetControlConn := proxy.GetTargetControlConn()

	originControlConnStatus := newControlConnStatus(originControlConn, proxy.Conf.HeartbeatFailureThreshold)
	targetControlConnStatus := newControlConnStatus(targetControlConn, proxy.Conf.HeartbeatFailureThreshold)
	status := UP
	if originControlConnStatus.Status != UP || targetControlConnStatus.Status != UP {
		status = DOWN
	}
	return &StatusReport{
		OriginStatus: originControlConnStatus,
		TargetStatus: targetControlConnStatus,
		Status:       status,
	}
}

func newControlConnStatus(controlConn *cloudgateproxy.ControlConn, failureThreshold int) *ControlConnStatus {
	currentEndpoint := controlConn.GetCurrentContactPoint()
	var addr string
	if currentEndpoint == nil {
		addr = "NOT_CONNECTED"
	} else {
		addr = currentEndpoint.GetEndpointIdentifier()
	}

	controlConnReport := &ControlConnStatus{
		Addr:                  addr,
		CurrentFailureCount:   controlConn.ReadFailureCounter(),
		FailureCountThreshold: failureThreshold,
		Status:                UP,
	}

	if controlConnReport.CurrentFailureCount >= controlConnReport.FailureCountThreshold {
		controlConnReport.Status = DOWN
	}

	return controlConnReport
}
