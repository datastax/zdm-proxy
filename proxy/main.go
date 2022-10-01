// +build !profiling

// Note: do NOT remove the blank line above, as it is needed by the build directive

package main

import (
	"flag"
	"fmt"
	"os"
)

var displayVersion = flag.Bool("version", false, "Display the zdm-proxy version and exit")

func main() {

	flag.Parse()
	if *displayVersion {
		fmt.Printf("zdm-proxy version %v\n", "2.0")
		os.Exit(0)
	}

	launchProxy(false)
}
