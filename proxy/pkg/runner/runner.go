package runner

import (
	"context"
	"errors"
	"fmt"
	"github.com/jpillora/backoff"
	"github.com/datastax/zdm-proxy/proxy/pkg/zdmproxy"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/health"
	"github.com/datastax/zdm-proxy/proxy/pkg/httpzdmproxy"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"net/http"
	"sync"
	"time"
)

func SetupHandlers() (metricsHandler *httpzdmproxy.HandlerWithFallback, readinessHandler *httpzdmproxy.HandlerWithFallback){
	metricsHandler = httpzdmproxy.NewHandlerWithFallback(metrics.DefaultHttpHandler())
	readinessHandler = httpzdmproxy.NewHandlerWithFallback(health.DefaultReadinessHandler())

	http.Handle("/metrics", metricsHandler.Handler())
	http.Handle("/health/readiness", readinessHandler.Handler())
	http.Handle("/health/liveness", health.LivenessHandler())
	return metricsHandler, readinessHandler
}

func RunMain(
	conf *config.Config,
	ctx context.Context,
	metricsHandler *httpzdmproxy.HandlerWithFallback,
	readinessHandler *httpzdmproxy.HandlerWithFallback) {

	log.Infof("Starting http server (metrics and health checks) on %v:%d", conf.ProxyMetricsAddress, conf.ProxyMetricsPort)
	wg := &sync.WaitGroup{}
	srv := httpzdmproxy.StartHttpServer(fmt.Sprintf("%s:%d", conf.ProxyMetricsAddress, conf.ProxyMetricsPort), wg)

	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: true,
	}

	cp, err := zdmproxy.RunWithRetries(conf, ctx, b)

	if err == nil {
		metricsHandler.SetHandler(cp.GetMetricHandler().GetHttpHandler())
		readinessHandler.SetHandler(health.ReadinessHandler(cp))

		log.Info("Proxy started. Waiting for SIGINT/SIGTERM to shutdown.")
		<-ctx.Done()

		cp.Shutdown()
		metricsHandler.ClearHandler()
		readinessHandler.ClearHandler()
	} else if !errors.Is(err, zdmproxy.ShutdownErr) {
		log.Errorf("Error launching proxy: %v", err)
	}

	log.Info("Shutting down httpzdmproxy server, waiting up to 5 seconds.")
	srvShutdownCtx, _ := context.WithTimeout(context.Background(), 5 * time.Second)
	if err := srv.Shutdown(srvShutdownCtx); err != nil {
		log.Errorf("Failed to gracefully shutdown httpzdmproxy server: %v", err)
	}

	wg.Wait()
	log.Info("Http server shutdown.")
}
