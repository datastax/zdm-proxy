package main

import (
	"context"
	"fmt"
	"github.com/jpillora/backoff"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Method mainly to test the proxy service for now
func main() {
	conf, err := config.New().ParseEnvVars()
	if err != nil {
		log.Errorf("Error loading configuration: %v. Aborting startup.", err)
		os.Exit(-1)
	}

	if conf.Debug {
		log.SetLevel(log.DebugLevel)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	runSignalListener(cancelFunc)
	log.Info("SIGINT/SIGTERM listener started.")

	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: true,
	}

	cp, err := cloudgateproxy.RunWithRetries(conf, ctx, b)

	if err != nil {
		return
	}

	log.Info("Starting http server.")
	wg := &sync.WaitGroup{}
	srv := startHttpServer(conf, wg)

	log.Info("Proxy started. Waiting for SIGINT/SIGTERM to shutdown.")
	<-ctx.Done()
	cp.Shutdown()

	log.Info("Shutting down http server, waiting up to 5 seconds.")
	metricsShutdownTimeoutCtx, _ := context.WithDeadline(context.Background(), time.Now().Add(5 * time.Second))
	if err := srv.Shutdown(metricsShutdownTimeoutCtx); err != nil {
		log.Errorf("Failed to gracefully shutdown http server: %v", err)
	}

	wg.Wait()
	log.Info("Http server shutdown.")
}

func runSignalListener(cancelFunc context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Debug("received signal:", sig)

		// let sub-task know to wrap up: cancel
		cancelFunc()
	}()
}

func startHttpServer(conf *config.Config, wg *sync.WaitGroup) *http.Server {
	srv := &http.Server{Addr: fmt.Sprintf("%s:%d", conf.ProxyMetricsAddress, conf.ProxyMetricsPort)}
	http.Handle("/metrics", promhttp.Handler())

	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Errorf("Failed to listen on the metrics endpoint: %v. " +
				"The proxy will stay up and listen for CQL requests.", err)
		}
	}()

	return srv
}