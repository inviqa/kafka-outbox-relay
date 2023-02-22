package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	nr "github.com/newrelic/go-agent/v3/newrelic"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/job"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/newrelic"
	"inviqa/kafka-outbox-relay/outbox"
	"inviqa/kafka-outbox-relay/outbox/data"
	"inviqa/kafka-outbox-relay/outbox/poller"
	"inviqa/kafka-outbox-relay/prometheus"
)

func main() {
	nrApp, stopAgent := newrelic.StartAgent()
	defer stopAgent()

	ctx, cancel := context.WithCancel(context.Background())
	cfg, err := config.NewConfig()
	if err != nil {
		log.Logger.Fatalf("unable to create configuration: %s", err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		cancel()
	}()

	dbs, dbClose := data.NewDBs(cfg)
	defer dbClose()

	var exitCode int
	switch {
	case cfg.RunCleanup:
		exitCode = job.RunCleanup(ctx, nrApp, dbs, cfg)
	case cfg.RunOptimize:
		exitCode = job.RunOptimize(ctx, nrApp, dbs, cfg)
	default:
		runMainApp(ctx, nrApp, dbs, cfg)
	}

	if exitCode > 0 {
		dbClose() // we call this manually because os.Exit() does not respect defer
		os.Exit(exitCode)
	}
}

func runMainApp(ctx context.Context, nrApp *nr.Application, dbs data.DBs, cfg *config.Config) {
	var repo outbox.Repository
	var cleanups []func()
	defer func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}()

	var sizers []prometheus.Sizer
	dbs.Each(func(db data.DB) {
		repo = outbox.NewRepository(db, cfg)
		cleanups = append(cleanups, poller.Start(ctx, cfg, repo, nrApp))
	})

	go prometheus.ObserveQueueSize(ctx, sizers)
	go prometheus.ObserveTotalSize(ctx, sizers)
	prometheus.StartHttpServer(ctx, cfg, dbs)
}
