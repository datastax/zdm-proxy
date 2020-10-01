package env

import (
	"os"
	"strings"
)

var ServerVersion string
var CassandraVersion string
var DseVersion string
var IsDse bool
var UseCcm bool

func init() {
	CassandraVersion = os.Getenv("CASSANDRA_VERSION")
	DseVersion = os.Getenv("DSE_VERSION")
	useCcm := os.Getenv("USE_CCM")
	if DseVersion != "" {
		IsDse = true
		ServerVersion = DseVersion
	} else if CassandraVersion != "" {
		ServerVersion = CassandraVersion
		IsDse = false
	} else {
		CassandraVersion = "3.11.7" // default
		ServerVersion = CassandraVersion
		IsDse = false
	}

	if strings.ToLower(useCcm) == "true" {
		UseCcm = true
	}
}
