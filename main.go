package main

import (
	"bufio"
	"cloud-gate/proxy"
	"flag"
	"fmt"
	"os"

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

func init() {
	parseFlags()

	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

}

// Method mainly to test the proxy service for now
func main() {
	// Channel for migration service to send a signal through, directing the proxy to forward all traffic directly
	// to the Astra DB
	migrationCompleteChannel := make(chan struct{})

	p := proxy.CQLProxy{
		SourceHostname: source_hostname,
		SourceUsername: source_username,
		SourcePassword: source_password,
		SourcePort:     source_port,

		AstraHostname: astra_hostname,
		AstraUsername: astra_username,
		AstraPassword: astra_password,
		AstraPort:     astra_port,

		ListenPort: listen_port,

		MigrationCompleteChan: migrationCompleteChannel,
	}

	// for testing purposes. to delete
	if test {
		go doTesting(&p)
	}

	p.Listen()

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
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter text: ")
		text, _ := reader.ReadString('\n')
		fmt.Println("entered:", text)
		p.DoTestToggle()
	}
}
