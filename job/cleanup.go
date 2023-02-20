package job

import (
	"context"
	"net/http"
	"time"

	nr "github.com/newrelic/go-agent/v3/newrelic"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/newrelic"
	"inviqa/kafka-outbox-relay/outbox"
	"inviqa/kafka-outbox-relay/outbox/data"
)

type publishedDeleter interface {
	DeletePublished(ctx context.Context, olderThan time.Time) (int64, error)
}

type cleanup struct {
	SidecarQuitter
	deleterFactory func() publishedDeleter
}

func RunCleanup(parent context.Context, nrApp *nr.Application, dbs data.DBs, cfg *config.Config) int {
	ctx, txn := newrelic.ContextWithTxn(parent, "run cleanup", nrApp)
	defer txn.End()

	var exitCode int
	dbs.Each(func(db data.DB) {
		exitCode += doCleanup(ctx, db, cfg)
	})
	return normalizeExitCode(exitCode)
}

func doCleanup(ctx context.Context, db data.DB, cfg *config.Config) int {
	txn := nr.FromContext(ctx)
	defer txn.StartSegment("doCleanup() " + db.Config().Driver.String()).End()

	j := newCleanupWithDefaults(db, cfg)

	if cfg.SidecarProxyUrl != "" {
		j.EnableSideCarProxyQuit(cfg.SidecarProxyUrl)
	}

	if err := j.Execute(ctx); err != nil {
		txn.NoticeError(err)
		return 1
	}

	return 0
}

func newCleanupWithDefaults(db data.DB, cfg *config.Config) *cleanup {
	return &cleanup{
		deleterFactory: func() publishedDeleter {
			return outbox.NewRepository(db, cfg)
		},
		SidecarQuitter: SidecarQuitter{
			Client: http.DefaultClient,
		},
	}
}

func (c *cleanup) Execute(ctx context.Context) error {
	rows, err := c.deleterFactory().DeletePublished(ctx, time.Now().Add(time.Duration(-1)*time.Hour))
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
