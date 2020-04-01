package main

import (
	"bufio"
	"cloud-gate/proxy"
	"flag"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

var (
	source_hostname string
	source_username string
	source_password string
	source_port     int

	astra_hostname string
	astra_username string
	astra_password string
	astra_port     int
	listen_port    int

	debug bool
	test  bool
)

// Method mainly to test the proxy service for now
func main() {
	parseFlags()

	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	// Channel for migrator to communicate with proxy when the migration process has begun
	migrationStartChan := make(chan *proxy.MigrationStatus, 1)

	// Channel for migration service to send a signal through, directing the proxy to forward all traffic directly
	// to the Astra DB
	migrationCompleteChan := make(chan struct{})

	p := proxy.CQLProxy{
		SourceHostname: source_hostname,
		SourceUsername: source_username,
		SourcePassword: source_password,
		SourcePort:     source_port,

		AstraHostname: astra_hostname,
		AstraUsername: astra_username,
		AstraPassword: astra_password,
		AstraPort:     astra_port,

		Port: listen_port,

		MigrationStartChan:    migrationStartChan,
		MigrationCompleteChan: migrationCompleteChan,
	}

	// for testing purposes. to delete
	if test {
		go doTesting(&p)
	}

	p.Start()
	waitForProxy(p)
	p.Listen()
}

func waitForProxy(p proxy.CQLProxy) {
	<-p.ReadyChan
	log.Info("Coordinator received proxy ready signal.")
}

// Most of these will change to environment variables rather than flags
func parseFlags() {
	flag.StringVar(&source_hostname, "source_hostname", "127.0.0.1", "Source Hostname")
	flag.StringVar(&source_username, "source_username", "", "Source Username")
	flag.StringVar(&source_password, "source_password", "", "Source Password")
	flag.IntVar(&source_port, "source_port", 9042, "Source Port")
	flag.StringVar(&astra_hostname, "astra_hostname", "127.0.0.1", "Astra Hostname")
	flag.StringVar(&astra_username, "astra_username", "", "Aster Username")
	flag.StringVar(&astra_password, "astra_password", "", "Astra Password")
	flag.IntVar(&astra_port, "astra_port", 9042, "Astra Port")
	flag.IntVar(&listen_port, "listen_port", 0, "Listening Port")
	flag.BoolVar(&debug, "debug", false, "Debug Mode")
	flag.BoolVar(&test, "test", false, "Test Mode")
	flag.Parse()
}

//function for testing purposes. Will be deleted later. toggles status for table 'codebase' upon user input
func doTesting(p *proxy.CQLProxy) {
	for {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			switch scanner.Text(){
			case "start":
				tables := make(map[string]proxy.Table)
				tables["tasks"] = proxy.Table{
					Name:     "tasks",
					Keyspace: "codebase",
					Status:   proxy.WAITING,
					Error:    nil,
				}

				p.MigrationStartChan <- &proxy.MigrationStatus{Tables: tables,
					Lock: sync.Mutex{}}
			case "complete":
				p.MigrationCompleteChan <- struct{}{}
			case "shutdown":
				p.ShutdownChan <- struct{}{}
			}
		}
	}
}
