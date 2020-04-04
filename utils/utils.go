package utils

import (
	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

func ConnectToCluster(hostname string, username string, password string, port int, keyspace string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(hostname)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	cluster.Port = port
	cluster.Keyspace = keyspace

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	log.Debugf("Connection established with Cluster: %s:%d", hostname, port)

	return session, nil
}

