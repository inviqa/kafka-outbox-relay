package newrelic

import (
	"os"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"

	"inviqa/kafka-outbox-relay/log"
)

const (
	shutdownTimeout   = time.Second * 10
	envKeyNewRelicEnv = "NEW_RELIC_ENV"
	envKeyLogLevel    = "NEW_RELIC_LOG_LEVEL"
)

func StartAgent() (*newrelic.Application, func()) {
	app, err := newrelic.NewApplication(
		newrelic.ConfigFromEnvironment(),
		agentLoggingConfig(),
		func(cfg *newrelic.Config) {
			cfg.Labels = map[string]string{
				"env": os.Getenv(envKeyNewRelicEnv),
			}
		},
	)
	if err != nil {
		log.Logger.WithError(err).Fatal("error starting New Relic agent")
	}
	return app, func() {
		log.Logger.Info("shutting down newrelic agent")
		app.Shutdown(shutdownTimeout)
	}
}

func agentLoggingConfig() newrelic.ConfigOption {
	if os.Getenv(envKeyLogLevel) == "debug" {
		return newrelic.ConfigDebugLogger(log.Logger.Writer())
	}
	return newrelic.ConfigInfoLogger(log.Logger.Writer())
}
