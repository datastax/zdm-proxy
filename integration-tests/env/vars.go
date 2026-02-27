package env

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

const (
	OriginNodes = 1
	TargetNodes = 1
)

var Rand = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
var ServerVersion string
var CassandraVersion string
var DseVersion string
var ServerVersionLogStr string
var IsDse bool
var RunCcmTests bool
var RunMockTests bool
var RunAllTlsTests bool
var Debug bool
var SupportedProtocolVersions []primitive.ProtocolVersion
var AllProtocolVersions []primitive.ProtocolVersion = []primitive.ProtocolVersion{
	primitive.ProtocolVersion2, primitive.ProtocolVersion3, primitive.ProtocolVersion4,
	primitive.ProtocolVersion5, primitive.ProtocolVersionDse1, primitive.ProtocolVersionDse2,
}
var DefaultProtocolVersion primitive.ProtocolVersion
var DefaultProtocolVersionSimulacron primitive.ProtocolVersion
var DefaultProtocolVersionTestClient primitive.ProtocolVersion

func InitGlobalVars() {
	flags := map[string]interface{}{
		"CASSANDRA_VERSION": flag.String(
			"CASSANDRA_VERSION",
			getEnvironmentVariableOrDefault("CASSANDRA_VERSION", "5.0.6"),
			"CASSANDRA_VERSION"),

		"DSE_VERSION": flag.String(
			"DSE_VERSION",
			getEnvironmentVariableOrDefault("DSE_VERSION", ""),
			"DSE_VERSION"),

		"RUN_CCMTESTS": flag.String(
			"RUN_CCMTESTS",
			getEnvironmentVariableOrDefault("RUN_CCMTESTS", "false"),
			"RUN_CCMTESTS"),

		"RUN_MOCKTESTS": flag.String(
			"RUN_MOCKTESTS",
			getEnvironmentVariableOrDefault("RUN_MOCKTESTS", "true"),
			"RUN_MOCKTESTS"),

		"RUN_ALL_TLS_TESTS": flag.String(
			"RUN_ALL_TLS_TESTS",
			getEnvironmentVariableOrDefault("RUN_ALL_TLS_TESTS", "false"),
			"RUN_ALL_TLS_TESTS"),

		"DEBUG": flag.Bool(
			"DEBUG",
			getEnvironmentVariableBoolOrDefault("DEBUG", false),
			"DEBUG"),
	}

	flag.Parse()

	CassandraVersion = *flags["CASSANDRA_VERSION"].(*string)
	DseVersion = *flags["DSE_VERSION"].(*string)
	runCcmTests := *flags["RUN_CCMTESTS"].(*string)
	runMockTests := *flags["RUN_MOCKTESTS"].(*string)
	runAllTlsTests := *flags["RUN_ALL_TLS_TESTS"].(*string)
	Debug = *flags["DEBUG"].(*bool)

	if DseVersion != "" {
		IsDse = true
		ServerVersion = DseVersion
	} else {
		split := strings.SplitAfter(CassandraVersion, "dse-")
		if len(split) == 2 {
			IsDse = true
			ServerVersion = split[1]
			DseVersion = ServerVersion
			CassandraVersion = ""
		} else {
			ServerVersion = CassandraVersion
			IsDse = false
		}
	}

	SupportedProtocolVersions = supportedProtocolVersions()

	ServerVersionLogStr = serverVersionLogString()

	DefaultProtocolVersion = ComputeDefaultProtocolVersion()

	if DefaultProtocolVersion <= primitive.ProtocolVersion2 {
		DefaultProtocolVersionSimulacron = primitive.ProtocolVersion3
	} else if DefaultProtocolVersion >= primitive.ProtocolVersion5 {
		DefaultProtocolVersionSimulacron = primitive.ProtocolVersion4
	} else {
		DefaultProtocolVersionSimulacron = DefaultProtocolVersion
	}

	if DefaultProtocolVersion.SupportsModernFramingLayout() {
		DefaultProtocolVersionTestClient = primitive.ProtocolVersion4
	} else {
		DefaultProtocolVersionTestClient = DefaultProtocolVersion
	}

	if strings.ToLower(runCcmTests) == "true" {
		RunCcmTests = true
	}

	if strings.ToLower(runMockTests) == "true" {
		RunMockTests = true
	}

	if strings.ToLower(runAllTlsTests) == "true" {
		RunAllTlsTests = true
	}
}

func CompareServerVersion(version string) int {
	v1 := parseVersion(ServerVersion)
	v2 := parseVersion(version)
	return compareVersion(v1, v2)
}

func parseVersion(version string) []int {
	// remove optional suffix (e.g. 5.0-beta1 becomes 5.0) and split version segments
	segmentsStr := strings.Split(strings.Split(version, "-")[0], ".")
	segments := make([]int, len(segmentsStr))
	for i, str := range segmentsStr {
		val, err := strconv.Atoi(str)
		if err != nil {
			return []int{0, 0, 0}
		}
		segments[i] = val
	}
	// if we have less than 3 segments, pad with zeros
	for i := len(segments); i < 3; i++ {
		segments = append(segments, 0)
	}
	return segments
}

func compareVersion(v1 []int, v2 []int) int {
	for i := 0; i < len(v1); i++ {
		if v1[i] == v2[i] {
			continue
		} else if v1[i] < v2[i] {
			return -1
		}
		return 1
	}
	return 0
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

func SupportsProtocolVersion(protoVersion primitive.ProtocolVersion) bool {
	return slices.Contains(SupportedProtocolVersions, protoVersion)
}

func supportedProtocolVersions() []primitive.ProtocolVersion {
	v := parseVersion(ServerVersion)
	if IsDse {
		if v[0] >= 6 {
			return []primitive.ProtocolVersion{
				primitive.ProtocolVersion3, primitive.ProtocolVersion4,
				primitive.ProtocolVersionDse1, primitive.ProtocolVersionDse2}
		}
		if v[0] >= 5 {
			return []primitive.ProtocolVersion{
				primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersionDse1}
		}

		if v[0] >= 4 {
			return []primitive.ProtocolVersion{primitive.ProtocolVersion2, primitive.ProtocolVersion3}
		}
	} else {
		if v[0] >= 4 {
			return []primitive.ProtocolVersion{
				primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersion5}
		}
		if v[0] >= 3 {
			return []primitive.ProtocolVersion{
				primitive.ProtocolVersion3, primitive.ProtocolVersion4}
		}
		if v[0] >= 2 {
			if v[1] >= 2 {
				return []primitive.ProtocolVersion{
					primitive.ProtocolVersion2, primitive.ProtocolVersion3, primitive.ProtocolVersion4}
			}

			if v[1] >= 1 {
				return []primitive.ProtocolVersion{primitive.ProtocolVersion2, primitive.ProtocolVersion3}
			}

			if v[1] >= 0 {
				return []primitive.ProtocolVersion{primitive.ProtocolVersion2}
			}
		}
	}

	panic(fmt.Sprintf("Unsupported server version IsDse=%v Version=%v", IsDse, ServerVersion))
}

func serverVersionLogString() string {
	if IsDse {
		return fmt.Sprintf("dse-%v", ServerVersion)
	} else {
		return ServerVersion
	}
}

func ProtocolVersionStr(v primitive.ProtocolVersion) string {
	switch v {
	case primitive.ProtocolVersionDse1:
		return "DSEv1"
	case primitive.ProtocolVersionDse2:
		return "DSEv2"
	}
	return strconv.Itoa(int(v))
}

func ComputeDefaultProtocolVersion() primitive.ProtocolVersion {
	orderedProtocolVersions := []primitive.ProtocolVersion{
		primitive.ProtocolVersionDse2, primitive.ProtocolVersionDse1, primitive.ProtocolVersion5,
		primitive.ProtocolVersion4, primitive.ProtocolVersion3, primitive.ProtocolVersion2}
	for _, v := range orderedProtocolVersions {
		if SupportsProtocolVersion(v) {
			return v
		}
	}
	panic(fmt.Sprintf("Unable to compute protocol version for server version %v", ServerVersionLogStr))
}
