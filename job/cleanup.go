package job

import (
	"net/http"
	"time"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox/data"
)

type publishedDeleter interface {
	DeletePublished(olderThan time.Time) (int64, error)
}

type cleanup struct {
	SidecarQuitter
	deleterFactory func() publishedDeleter
}

func RunCleanup(dbs data.DBs, cfg *config.Config) int {
	var exitCode int
	dbs.Each(func(db data.DB) {
		exitCode += doCleanup(cfg)
	})
	return normalizeExitCode(exitCode)
}

func doCleanup(cfg *config.Config) int {
	j := newCleanupWithDefaultClient()

	if cfg.SidecarProxyUrl != "" {
		j.EnableSideCarProxyQuit(cfg.SidecarProxyUrl)
	}

	if err := j.Execute(); err != nil {
		return 1
	}

	return 0
}

func newCleanupWithDefaultClient() *cleanup {
	return &cleanup{
		SidecarQuitter: SidecarQuitter{
			Client: http.DefaultClient,
		},
	}
}

func newCleanup(cl httpPoster) *cleanup {
	return &cleanup{
		SidecarQuitter: SidecarQuitter{
			Client: cl,
		},
	}
}

func (c *cleanup) Execute() error {
	rows, err := c.deleterFactory().DeletePublished(time.Now().Add(time.Duration(-1) * time.Hour))
	if err != nil {
		log.Logger.WithError(err).Error("an error occurred whilst deleting published outbox records")
		return err
	}

	log.Logger.Infof("deleted %d published outbox records", rows)

	if c.QuitSidecar {
		if err = c.Quit(); err != nil {
			return err
		}
	}
	return nil
}
