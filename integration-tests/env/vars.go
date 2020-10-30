package env

import (
	"flag"
	"math/rand"
	"os"
	"strings"
	"time"
)

const (
	OriginNodes = 1
	TargetNodes = 1
)

var Rand = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
var ServerVersion string
var CassandraVersion string
var DseVersion string
var IsDse bool
var UseCcm bool

func InitGlobalVars() {
	flags := map[string]*string{
		"CASSANDRA_VERSION":
		flag.String(
			"CASSANDRA_VERSION",
			getEnvironmentVariableOrDefault("CASSANDRA_VERSION", "3.11.7"),
			"CASSANDRA_VERSION"),

		"DSE_VERSION":
		flag.String(
			"DSE_VERSION",
			getEnvironmentVariableOrDefault("DSE_VERSION", ""),
			"DSE_VERSION"),

		"USE_CCM":
		flag.String(
			"USE_CCM",
			getEnvironmentVariableOrDefault("USE_CCM", "false"),
			"USE_CCM"),
	}

	flag.Parse()

	CassandraVersion = *flags["CASSANDRA_VERSION"]
	DseVersion = *flags["DSE_VERSION"]
	useCcm := *flags["USE_CCM"]
	if DseVersion != "" {
		IsDse = true
		ServerVersion = DseVersion
	} else {
		ServerVersion = CassandraVersion
		IsDse = false
	}

	if strings.ToLower(useCcm) == "true" {
		UseCcm = true
	}
}

func getEnvironmentVariableOrDefault(key string, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	} else {
		return defaultValue
	}
}
