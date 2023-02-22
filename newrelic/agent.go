package newrelic

import (
	"os"
	"strconv"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"

	"inviqa/kafka-outbox-relay/log"
)

const (
	shutdownTimeout       = time.Second * 10
	envKeyNewRelicEnv     = "NEW_RELIC_ENV"
	envKeyLogLevel        = "NEW_RELIC_LOG_LEVEL"
	envKeyNewRelicEnabled = "NEW_RELIC_ENABLED"
)

func StartAgent() (*newrelic.Application, func()) {
	if !isAgentEnabled() {
		return nil, func() {}
	}

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
		app.Shutdown(shutdownTimeout)
	}
}

func agentLoggingConfig() newrelic.ConfigOption {
	if os.Getenv(envKeyLogLevel) == "debug" {
		return newrelic.ConfigDebugLogger(log.Logger.Writer())
	}
	return newrelic.ConfigInfoLogger(log.Logger.Writer())
}

func isAgentEnabled() bool {
	value := os.Getenv(envKeyNewRelicEnabled)
	if value == "" {
		return false
	}

	enabled, err := strconv.ParseBool(value)
	if err != nil {
		log.Logger.WithError(err).Fatalf("could not parse environment variable: %s", envKeyNewRelicEnabled)
	}

	return enabled
}
