package config

import "os"

type envVar struct {
	vName  string
	vValue string
}

func setOriginCredentialsEnvVars() {
	setEnvVar("ORIGIN_CASSANDRA_USERNAME", "originUser")
	setEnvVar("ORIGIN_CASSANDRA_PASSWORD", "originPassword")
}

func setTargetCredentialsEnvVars() {
	setEnvVar("TARGET_CASSANDRA_USERNAME", "targetUser")
	setEnvVar("TARGET_CASSANDRA_PASSWORD", "targetPassword")
}

func setOriginContactPointsAndPortEnvVars() {
	setEnvVar("ORIGIN_CASSANDRA_CONTACT_POINTS", "origin.hostname.com")
	setEnvVar("ORIGIN_CASSANDRA_PORT", "7890")
}

func setTargetContactPointsAndPortEnvVars() {
	setEnvVar("TARGET_CASSANDRA_CONTACT_POINTS", "target.hostname.com")
	setEnvVar("TARGET_CASSANDRA_PORT", "5647")
}

func setOriginSecureConnectBundleEnvVar() {
	setEnvVar("ORIGIN_CASSANDRA_SECURE_CONNECT_BUNDLE_PATH", "/path/to/origin/bundle")
}

func setTargetSecureConnectBundleEnvVar() {
	setEnvVar("TARGET_CASSANDRA_SECURE_CONNECT_BUNDLE_PATH", "/path/to/origin/bundle")
}

func setEnvVar(key string, value string) {
	os.Setenv(key, value)
}

func clearAllEnvVars() {
	os.Clearenv()
}
