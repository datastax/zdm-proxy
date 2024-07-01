//go:build profiling
// +build profiling

// Note: do NOT remove the blank line above, as it is needed by the build directive

package main

import (
	"flag"
	log "github.com/sirupsen/logrus"
	"os"
	"runtime"
	"runtime/pprof"
)

var cpuProfile = flag.String("cpu_profile", "", "write cpu profile to the specified file")
var memProfile = flag.String("mem_profile", "", "write memory profile to the specified file")
var configFile = flag.String("config", "", "specify path to ZDM configuration file")

func main() {

	flag.Parse()

	// the cpu profiling is enabled at startup and is periodically collected while the proxy is running
	// if cpu profiling is requested, any error configuring or starting it will cause the proxy startup to fail
	if *cpuProfile != "" {
		log.Infof("CPU Profiling enabled")
		cpuFile, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatalf("Proxy startup failed. An error occurred while attempting to create a file to store the CPU profiling: %v", err)
		}
		err = pprof.StartCPUProfile(cpuFile)
		if err != nil {
			log.Fatalf("Proxy startup failed. An error occurred while starting the CPU profiling: %v ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *memProfile != "" {
		log.Infof("Memory Profiling enabled")

		// the memory profile collection only happens at proxy shutdown
		// if memory profiling is requested, any error collecting it will simply be logged when the proxy is shut down
		defer func() {
			profile := pprof.Lookup("allocs")
			if profile == nil {
				log.Errorf("Memory profiling failed. The memory profiling information was not found")
				return
			}

			memFile, err := os.Create(*memProfile)
			if err != nil {
				log.Errorf("Memory profiling failed. An error occurred while creating a file to store the memory profiling information: %v", err)
				return
			}

			runtime.GC()
			err = profile.WriteTo(memFile, 0)
			if err != nil {
				log.Errorf("Memory profiling failed. An error occurred while attempting to write to file the memory profiling information: %v", err)
			}
		}()
	}

	launchProxy(true, configFile)
}
