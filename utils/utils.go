package utils

import (
	"os"
	"path/filepath"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
	"strconv"
	"unicode"
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

// DirSize returns the size of the directory in bytes
func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
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

// checkUpper returns true if any letters are uppercase
func checkUpper(name string) bool {
	runes := []rune(name)
	for _, r := range runes {
		if unicode.IsUpper(r) {
			return true
		}
	}
	return false
}

// DsbulkUpper returns a properly formatted string if any letters are uppercase
func DsbulkUpper(name string) string {
	if checkUpper(name) {
		return "\\\"" + name + "\\\""
	}
	return name
}

// HasUpper returns the string in quotation marks if any letters are uppercase
func HasUpper(name string) string {
	if checkUpper(name) {
		return strconv.Quote(name)
	}
	return name
}
