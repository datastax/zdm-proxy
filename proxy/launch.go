package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/runner"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

// TODO: to be managed externally
const ZdmVersionString = "2.3.3"

var displayVersion = flag.Bool("version", false, "display the ZDM proxy version and exit")
var configFile = flag.String("config", "", "specify path to ZDM configuration file")

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

func launchProxy(profilingSupported bool) {
	if *displayVersion {
		fmt.Printf("ZDM proxy version %v\n", ZdmVersionString)
		return
	}

	// Always record version information (very) early in the log
	log.Infof("Starting ZDM proxy version %v", ZdmVersionString)

	conf, err := config.New().LoadConfig(*configFile)

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
