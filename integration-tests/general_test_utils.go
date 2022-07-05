package integration_tests

import (
	"github.com/riptano/cloud-gate/integration-tests/utils"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/writer"
)

func createLogHooks(logLevels ...log.Level) *utils.ThreadsafeBuffer {
	buffer := utils.NewThreadsafeBuffer()
	hook := &writer.Hook{
		Writer:    buffer,
		LogLevels: logLevels,
	}
	log.AddHook(hook)
	return buffer
}
