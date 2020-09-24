package main

import (
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/test"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"

	"fmt"

	"github.com/riptano/cloud-gate/utils"

	"github.com/gocql/gocql"

	log "github.com/sirupsen/logrus"
)

func main() {
	gocql.TimeoutLimit = 5
	log.SetLevel(log.DebugLevel)

	// Connect to source and dest sessions
	var err error
	sourceSession, err := utils.ConnectToCluster("127.0.0.1", "", "", 9042)
	if err != nil {
		log.WithError(err).Error("Error connecting to source cluster.")
	}
	defer sourceSession.Close()

	destSession, err := utils.ConnectToCluster("127.0.0.1", "", "", 9043)
	if err != nil {
		log.WithError(err).Error("Error connecting to dest cluster.")
	}
	defer destSession.Close()

	// Seed source and dest with keyspace
	setup.SeedKeyspace(sourceSession, destSession)

	conf := &config.Config{
		OriginCassandraHostname: "127.0.0.1",
		OriginCassandraUsername: "",
		OriginCassandraPassword: "",
		OriginCassandraPort:     9042,

		TargetCassandraHostname: "127.0.0.1",
		TargetCassandraUsername: "",
		TargetCassandraPassword: "",
		TargetCassandraPort:     9043,

		ProxyServiceHostname:       "127.0.0.1",
		ProxyCommunicationPort:     14000,
		ProxyMetricsPort:           8080,
		ProxyQueryPort:             14002,

		Test:         false,
		Debug:        true,
		MaxQueueSize: 1000,
	}

	p := &cloudgateproxy.CloudgateProxy{
		Conf: conf,
	}

	go p.Start()

	log.Info("PROXY STARTED")

	go setup.ListenProxy()

	// Establish connection w/ proxy
	conn := setup.EstablishConnection(fmt.Sprintf("127.0.0.1:14000"))

	// Run test package here
	// test.BasicUpdate(conn, sourceSession, destSession)
	test.BasicBatch(conn, sourceSession, destSession)
	// test.QueueBatch(conn, sourceSession, destSession)
}
