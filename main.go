package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/job"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox"
	"inviqa/kafka-outbox-relay/outbox/data"
	"inviqa/kafka-outbox-relay/outbox/poller"
	"inviqa/kafka-outbox-relay/prometheus"
)

func main() {
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
		exitCode = job.RunCleanup(dbs, cfg)
	case cfg.RunOptimize:
		exitCode = job.RunOptimize(dbs, cfg)
	default:
		runMainApp(dbs, cfg, ctx)
	}

	if exitCode > 0 {
		dbClose() // we call this manually because os.Exit() does not respect defer
		os.Exit(exitCode)
	}
}

func runMainApp(dbs data.DBs, cfg *config.Config, ctx context.Context) {
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
		cleanups = append(cleanups, poller.Start(cfg, repo, ctx))
	})

	go prometheus.ObserveQueueSize(sizers, ctx)
	go prometheus.ObserveTotalSize(sizers, ctx)
	prometheus.StartHttpServer(ctx, cfg, dbs)
}
