// Use package main for testing purposes (so that we can run these),
// However, we should probably create a controller package or something
// for the final product
package main

import (
	"flag"

	"cloud-gate/proxy"
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
)

func main() {
	parseFlags()
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
	}

	p.Listen()
}

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
	flag.Parse()
}
