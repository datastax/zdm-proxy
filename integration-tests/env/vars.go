package env

import (
	"flag"
	"math/rand"
	"os"
	"strconv"
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
var SkipMockTests bool
var RunAllTlsTests bool
var Debug bool

func InitGlobalVars() {
	flags := map[string]interface{}{
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

		"SKIP_MOCKTESTS":
		flag.String(
			"SKIP_MOCKTESTS",
			getEnvironmentVariableOrDefault("SKIP_MOCKTESTS", "false"),
			"SKIP_MOCKTESTS"),

		"RUN_ALL_TLS_TESTS":
		flag.String(
			"RUN_ALL_TLS_TESTS",
			getEnvironmentVariableOrDefault("RUN_ALL_TLS_TESTS", "false"),
			"RUN_ALL_TLS_TESTS"),

		"DEBUG":
		flag.Bool(
			"DEBUG",
			getEnvironmentVariableBoolOrDefault("DEBUG", false),
			"DEBUG"),
	}

	flag.Parse()

	CassandraVersion = *flags["CASSANDRA_VERSION"].(*string)
	DseVersion = *flags["DSE_VERSION"].(*string)
	useCcm := *flags["USE_CCM"].(*string)
	skipMockTests := *flags["SKIP_MOCKTESTS"].(*string)
	runAllTlsTests := *flags["RUN_ALL_TLS_TESTS"].(*string)
	Debug = *flags["DEBUG"].(*bool)

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

	if strings.ToLower(skipMockTests) == "true" {
		SkipMockTests = true
	}

	if strings.ToLower(runAllTlsTests) == "true" {
		RunAllTlsTests = true
	}
}

func getEnvironmentVariableOrDefault(key string, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	} else {
		return defaultValue
	}
}

func getEnvironmentVariableBoolOrDefault(key string, defaultValue bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		result, err := strconv.ParseBool(value)
		if err != nil {
			return defaultValue
		} else {
			return result
		}
	} else {
		return defaultValue
	}
}
