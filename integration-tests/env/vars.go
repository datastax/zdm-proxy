package env

import (
	"os"
	"strings"
)

var ServerVersion string
var CassandraVersion string
var DseVersion string
var IsDse bool
var UseCcmGlobal bool

func init() {
	CassandraVersion = os.Getenv("CASSANDRA_VERSION")
	DseVersion = os.Getenv("DSE_VERSION")
	ccmGlobal := os.Getenv("USE_CCM_GLOBAL")
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

	if strings.ToLower(ccmGlobal) == "true" {
		UseCcmGlobal = true
	}
}