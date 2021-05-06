package job

import (
	"inviqa/kafka-outbox-relay/config"
	"net/http"
	"time"

	"inviqa/kafka-outbox-relay/log"
)

type PublishedDeleter interface {
	DeletePublished(olderThan time.Time) (int64, error)
}

type cleanup struct {
	pd PublishedDeleter
	SidecarQuitter
}

func RunCleanup(repo PublishedDeleter, cfg *config.Config) int {
	j := newCleanupWithDefaultClient(repo)
	if cfg.SidecarProxyUrl != "" {
		j.EnableSideCarProxyQuit(cfg.SidecarProxyUrl)
	}

	_, err := j.Execute()
	if err != nil {
		return 1
	}

	return 0
}

func newCleanupWithDefaultClient(pd PublishedDeleter) *cleanup {
	return &cleanup{
		pd: pd,
		SidecarQuitter: SidecarQuitter{
			Client: http.DefaultClient,
		},
	}
}

func newCleanup(pd PublishedDeleter, cl httpPoster) *cleanup {
	return &cleanup{
		pd: pd,
		SidecarQuitter: SidecarQuitter{
			Client: cl,
		},
	}
}

func (c *cleanup) Execute() (int64, error) {
	rows, err := c.pd.DeletePublished(time.Now().Add(time.Duration(-1) * time.Hour))
	if err != nil {
		log.Logger.WithError(err).Error("an error occurred whilst deleting published outbox records")
		return 0, err
	}

	log.Logger.Infof("deleted %d published outbox records", rows)

	if c.QuitSidecar {
		err = c.Quit()
		if err != nil {
			return 0, err
		}
	}

	return rows, nil
}
