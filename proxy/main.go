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

	// HTTP Handler to expose the metrics endpoint used by Prometheus to pull metrics
	http.Handle("/metrics", promhttp.Handler())
	err = http.ListenAndServe(fmt.Sprintf("%s:%d", conf.ProxyMetricsAddress, conf.ProxyMetricsPort), nil)
	log.Errorf("Failed to listen on the metrics endpoint: %v. The proxy will stay up and listen for CQL requests.", err)

	log.Info("Proxy started. Waiting for SIGINT/SIGTERM to shutdown.")
	<-ctx.Done()

	cp.Shutdown()
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
