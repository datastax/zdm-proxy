package main

import (
	"context"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

// Method mainly to test the proxy service for now
func main() {
	conf := config.New().ParseEnvVars()

	if conf.Debug {
		log.SetLevel(log.DebugLevel)
	}
	log.Debugf("parsed env vars")

	cp := cloudgateproxy.Run(conf)

	// HTTP Handler to expose the metrics endpoint used by Prometheus to pull metrics
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":8080", nil))

	log.Info("Started, waiting for SIGINT/SIGTERM")
	<-registerSigHandler().Done()
	log.Info("Shutting down proxy...")
	cp.Shutdown()
}

func registerSigHandler() context.Context {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	rootCtx := context.Background()
	taskCtx, cancelFn := context.WithCancel(rootCtx)

	go func() {
		sig := <-sigCh
		log.Debug("received signal:", sig)

		// let sub-task know to wrap up: cancel taskCtx
		cancelFn()
	}()

	return taskCtx
}
