package runner

import (
	"context"
	"errors"
	"fmt"
	"github.com/jpillora/backoff"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/health"
	"github.com/riptano/cloud-gate/proxy/pkg/httpcloudgate"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"net/http"
	"sync"
	"time"
)

func SetupHandlers() (metricsHandler *httpcloudgate.HandlerWithFallback, readinessHandler *httpcloudgate.HandlerWithFallback){
	metricsHandler = httpcloudgate.NewHandlerWithFallback(metrics.DefaultHandler())
	readinessHandler = httpcloudgate.NewHandlerWithFallback(health.DefaultReadinessHandler())

	http.Handle("/metrics", metricsHandler.Handler())
	http.Handle("/health/readiness", readinessHandler.Handler())
	http.Handle("/health/liveness", health.LivenessHandler())
	return metricsHandler, readinessHandler
}

func RunMain(
	conf *config.Config,
	ctx context.Context,
	metricsHandler *httpcloudgate.HandlerWithFallback,
	readinessHandler *httpcloudgate.HandlerWithFallback) {

	log.Info("Starting http server.")
	wg := &sync.WaitGroup{}
	srv := httpcloudgate.StartHttpServer(fmt.Sprintf("%s:%d", conf.ProxyMetricsAddress, conf.ProxyMetricsPort), wg)

	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: true,
	}

	cp, err := cloudgateproxy.RunWithRetries(conf, ctx, b)

	if err == nil {
		metricsHandler.SetHandler(promhttp.Handler())
		readinessHandler.SetHandler(health.ReadinessHandler(cp))
		log.Info("Proxy started. Waiting for SIGINT/SIGTERM to shutdown.")

		<-ctx.Done()

		cp.Shutdown()
	} else if !errors.Is(err, cloudgateproxy.ShutdownErr){
		log.Errorf("Error launching proxy: %v", err)
	}

	log.Info("Shutting down httpcloudgate server, waiting up to 5 seconds.")
	srvShutdownCtx, _ := context.WithTimeout(context.Background(), 5 * time.Second)
	if err := srv.Shutdown(srvShutdownCtx); err != nil {
		log.Errorf("Failed to gracefully shutdown httpcloudgate server: %v", err)
	}

	wg.Wait()
	log.Info("Http server shutdown.")
}
