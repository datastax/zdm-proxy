package utils

import (
	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

// ConnectToCluster is used to connect to source and destination clusters
func ConnectToCluster(hostname string, username string, password string, port int) (*gocql.Session, error) {
	cluster := gocql.NewCluster(hostname)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	cluster.Port = port

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	log.Debugf("Connection established with Cluster: %s:%d", hostname, port)

	return session, nil
}

// Contains checks if an element exists in a slice
func Contains(keys []string, elem string) bool {
	for _, key := range keys {
		if key == elem {
			return true
		}
	}
	return false
}
