package test

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

// Assert ensures the two given values are equal, otherwise errors
func Assert(expected interface{}, actual interface{}) {
	if expected != actual {
		log.Error(fmt.Sprintf("Assertion failed:\nReceived: %s\nExpected: %s", actual, expected))
	}
}
