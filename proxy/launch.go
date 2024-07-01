package main

import (
	"context"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/runner"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

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

func launchProxy(profilingSupported bool, configFile string) {
	conf, err := config.New().LoadConfig(configFile)

	if err != nil {
		log.Errorf("Error loading configuration: %v. Aborting startup.", err)
		os.Exit(-1)
	}

	logLevel, err := conf.ParseLogLevel()
	if err != nil {
		log.Errorf("Error loading log level configuration: %v. Aborting startup.", err)
		os.Exit(-1)
	}
	log.SetLevel(logLevel)

	if profilingSupported {
		log.Debugf("Proxy built with profiling support")
	} else {
		log.Debugf("Proxy built with regular build")
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	runSignalListener(cancelFunc)
	log.Info("SIGINT/SIGTERM listener started.")

	metricsHandler, readinessHandler := runner.SetupHandlers()
	runner.RunMain(conf, ctx, metricsHandler, readinessHandler)
}
