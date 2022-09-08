package config

import "os"

type envVar struct {
	vName  string
	vValue string
}

func setOriginCredentialsEnvVars() {
	setEnvVar("ZDM_ORIGIN_USERNAME", "originUser")
	setEnvVar("ZDM_ORIGIN_PASSWORD", "originPassword")
}

func setTargetCredentialsEnvVars() {
	setEnvVar("ZDM_TARGET_USERNAME", "targetUser")
	setEnvVar("ZDM_TARGET_PASSWORD", "targetPassword")
}

func setOriginContactPointsAndPortEnvVars() {
	setEnvVar("ZDM_ORIGIN_CONTACT_POINTS", "origin.hostname.com")
	setEnvVar("ZDM_ORIGIN_PORT", "7890")
}

func setTargetContactPointsAndPortEnvVars() {
	setEnvVar("ZDM_TARGET_CONTACT_POINTS", "target.hostname.com")
	setEnvVar("ZDM_TARGET_PORT", "5647")
}

func setOriginSecureConnectBundleEnvVar() {
	setEnvVar("ZDM_ORIGIN_SECURE_CONNECT_BUNDLE_PATH", "/path/to/origin/bundle")
}

func setTargetSecureConnectBundleEnvVar() {
	setEnvVar("ZDM_TARGET_SECURE_CONNECT_BUNDLE_PATH", "/path/to/origin/bundle")
}

func setEnvVar(key string, value string) {
	os.Setenv(key, value)
}

func clearAllEnvVars() {
	os.Clearenv()
}