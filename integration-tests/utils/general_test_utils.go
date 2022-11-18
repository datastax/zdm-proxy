package utils

import (
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/writer"
	"os"
)

func CreateLogHooks(logLevels ...log.Level) *ThreadsafeBuffer {
	buffer := NewThreadsafeBuffer()
	hook := &writer.Hook{
		Writer:    buffer,
		LogLevels: logLevels,
	}
	log.AddHook(hook)
	return buffer
}

func CreateZeroLogHooks(logLevel zerolog.Level) *ThreadsafeBuffer {
	buffer := NewThreadsafeBuffer()
	zerologger.Logger = zerolog.New(zerolog.MultiLevelWriter(os.Stderr, &LevelWriter{Writer: buffer, Level: logLevel})).With().Timestamp().Logger()
	return buffer
}

func ResetZeroLog() {
	zerologger.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
}
