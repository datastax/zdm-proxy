package setup

import (
	"fmt"
	"testing"
)

// Assert ensures the two given values are equal, otherwise errors
func AssertEqual(t *testing.T, expected interface{}, actual interface{}) {
	AssertEqualWithMessage(
		t,
		expected,
		actual,
		fmt.Sprintf("Assertion failed:\nReceived: %s\nExpected: %s", actual, expected))
}

// Assert ensures the two given values are equal, otherwise errors
func AssertEqualWithMessage(t *testing.T, expected interface{}, actual interface{}, message string) {
	if expected != actual {
		t.Fatal(message)
	}
}
