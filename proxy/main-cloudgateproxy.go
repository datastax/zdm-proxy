package main

import (
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"github.com/riptano/cloud-gate/utils"


	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

var (
	cp *cloudgateproxy.CloudgateProxy
)

// Method mainly to test the proxy service for now
func main() {
	conf := config.New().ParseEnvVars()

	if conf.Debug {
		log.SetLevel(log.DebugLevel)
	}
	log.Debugf("parsed env vars")

	cp = &cloudgateproxy.CloudgateProxy{
		Conf: conf,
	}

	m := new(CPMetadata)

	err := m.Init(cp)
	if err != nil {
		log.WithError(err).Fatal("Proxy initialization failed when attempting to connect to the clusters")
	}


	err2 := cp.Start()
	if err2 != nil {
		// TODO: handle error
		log.Error(err2)
		panic(err)
	}

	log.Debugf("Started, waiting for ReadyForRedirect in background")
	for {
		select {
		case <-cp.ReadyForRedirect:
			log.Info("Coordinate received signal that there are no more connections to Client Database.")
		}
	}

}

func (m *CPMetadata) Init(cp *cloudgateproxy.CloudgateProxy) error {

	// Connect to source and destination sessions w/ gocql
	var err error
	m.sourceSession, err = utils.ConnectToCluster(cp.Conf.OriginCassandraHostname, cp.Conf.OriginCassandraUsername, cp.Conf.OriginCassandraPassword, cp.Conf.OriginCassandraPort)
	if err != nil {
		return err
	}

	m.destSession, err = utils.ConnectToCluster(cp.Conf.TargetCassandraHostname, cp.Conf.TargetCassandraUsername, cp.Conf.TargetCassandraPassword, cp.Conf.TargetCassandraPort)
	if err != nil {
		return err
	}
	return nil
}

// TODO: remove struct
type CPMetadata struct {
	// Sessions TODO rename these more clearly
	sourceSession *gocql.Session
	destSession   *gocql.Session

	// Data
	keyspaces []string
}
