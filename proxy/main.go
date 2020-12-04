package main

import (
	"context"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/runner"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
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

	metricsHandler, readinessHandler := runner.SetupHandlers()
	runner.RunMain(conf, ctx, metricsHandler, readinessHandler)
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