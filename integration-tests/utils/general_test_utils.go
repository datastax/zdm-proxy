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

type InMemoryHook struct{
	levels []zerolog.Level
	buffer *ThreadsafeBuffer
}

func (h *InMemoryHook) Run(_ *zerolog.Event, level zerolog.Level, msg string) {
	for _, l := range h.levels {
		if level == l {
			h.buffer.Write([]byte("level=" + level.String() + " " + msg))
			return
		}
	}
}

func CreateZeroLogHooks(logLevels ...zerolog.Level) *ThreadsafeBuffer {
	buffer := NewThreadsafeBuffer()
	newLogger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	newLogger.Hook(&InMemoryHook{
		levels: logLevels,
		buffer: buffer,
	})
	return buffer
}

func ResetZeroLog() {
	zerologger.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
}