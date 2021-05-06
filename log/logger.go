package log

import (
	"os"

	"github.com/sirupsen/logrus"
)

const defaultLevel = logrus.ErrorLevel

var Logger logrus.FieldLogger

func init() {
	Logger = newLogger(os.Getenv("LOG_LEVEL"))
}

func newLogger(level string) logrus.FieldLogger {
	l := logrus.New()
	l.Formatter = &logrus.JSONFormatter{}
	l.Out = os.Stdout

	lvl, err := resolveLogLevel(level)
	l.Level = lvl

	if err != nil {
		l.Errorf("an error occurred resolving the log level: %s", err)
	}

	return l
}

func resolveLogLevel(envLvl string) (logrus.Level, error) {
	if envLvl == "" {
		return defaultLevel, nil
	}

	lvl, err := logrus.ParseLevel(envLvl)
	if err != nil {
		return defaultLevel, err
	}

	return lvl, nil
}
