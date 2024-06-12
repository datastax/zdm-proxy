//go:build !profiling
// +build !profiling

// Note: do NOT remove the blank line above, as it is needed by the build directive

package main

import (
	"flag"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
)

// TODO: to be managed externally
const ZdmVersionString = "2.2.0"

var displayVersion = flag.Bool("version", false, "Display the ZDM proxy version and exit")

func main() {

	flag.Parse()
	if *displayVersion {
		fmt.Printf("ZDM proxy version %v\n", ZdmVersionString)
		os.Exit(0)
	}

	// Always record version information (very) early in the log
	log.Infof("Starting ZDM proxy version %v", ZdmVersionString)

	launchProxy(false)
}
