package main

import (
	"cloud-gate/migration"
	"flag"

	log "github.com/sirupsen/logrus"
)

var (
	sourceHostname string
	sourceUsername string
	sourcePassword string
	sourcePort     int

	astraHostname string
	astraUsername string
	astraPassword string
	astraPort     int

	dsbulkPath  string
	hardRestart bool
	threads     int
	proxyPort   int
)

// Method mainly to test the proxy service for now
func main() {
	parseFlags()

	m := migration.Migration{
		DsbulkPath:  dsbulkPath,
		HardRestart: hardRestart,
		Workers:     threads,
		ProxyPort:   proxyPort,

		SourceHostname: sourceHostname,
		SourceUsername: sourceUsername,
		SourcePassword: sourcePassword,
		SourcePort:     sourcePort,

		DestHostname: astraHostname,
		DestUsername: astraUsername,
		DestPassword: astraPassword,
		DestPort:     astraPort,
	}

	err := m.Init()
	if err != nil {
		log.Fatal(err)
	}

	go m.Migrate()

	for {
	}
}

// Most of these will change to environment variables rather than flags
func parseFlags() {
	flag.StringVar(&sourceHostname, "source_hostname", "127.0.0.1", "Source Hostname")
	flag.StringVar(&sourceUsername, "source_username", "", "Source Username")
	flag.StringVar(&sourcePassword, "source_password", "", "Source Password")
	flag.IntVar(&sourcePort, "source_port", 9042, "Source Port")
	flag.StringVar(&astraHostname, "astra_hostname", "127.0.0.1", "Astra Hostname")
	flag.StringVar(&astraUsername, "astra_username", "", "Aster Username")
	flag.StringVar(&astraPassword, "astra_password", "", "Astra Password")
	flag.IntVar(&astraPort, "astra_port", 9042, "Astra Port")
	flag.StringVar(&dsbulkPath, "d", "/Users/terranceli/Documents/projects/codebase/datastax-s20/dsbulk-1.4.1/bin/dsbulk", "dsbulk executable path")
	flag.BoolVar(&hardRestart, "r", false, "Hard restart (ignore checkpoint)")
	flag.IntVar(&threads, "t", 1, "Number of threads to use")
	flag.IntVar(&proxyPort, "pp", 0, "Port of the proxy service")
	flag.Parse()
}
