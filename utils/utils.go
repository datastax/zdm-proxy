package utils

import (
	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

// ConnectToCluster is used to connect to source and destination clusters
func ConnectToCluster(hostname string, username string, password string, port int) (*gocql.Session, error) {
	cluster := NewCluster(hostname, username, password, port)
	session, err := cluster.CreateSession()
	log.Debugf("Connection established with Cluster: %s:%d", cluster.Hosts[0], cluster.Port)
	if err != nil {
		return nil, err
	}
	return session, nil
}

// NewCluster initializes a ClusterConfig object with common settings
func NewCluster(hostname string, username string, password string, port int) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hostname)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	cluster.Port = port
	return cluster
}
