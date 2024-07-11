//go:build !profiling
// +build !profiling

// Note: do NOT remove the blank line above, as it is needed by the build directive

package main

import (
	"flag"
)

func main() {
	flag.Parse()

	launchProxy(false)
}
