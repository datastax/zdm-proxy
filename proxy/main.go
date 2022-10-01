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
const ZdmVersionNumber = "2.0"

var displayVersion = flag.Bool("version", false, "Display the zdm-proxy version and exit")

func main() {

	flag.Parse()
	if *displayVersion {
		fmt.Printf("zdm-proxy version %v\n", ZdmVersionNumber)
		os.Exit(0)
	}

	// Always record version information (very) early in the log
	log.Infof("zdm-proxy version %v", ZdmVersionNumber)

	launchProxy(false)
}
