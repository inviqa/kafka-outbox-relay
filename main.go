package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/job"
	"inviqa/kafka-outbox-relay/kafka"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox"
	"inviqa/kafka-outbox-relay/outbox/data"
	"inviqa/kafka-outbox-relay/outbox/poller"
	"inviqa/kafka-outbox-relay/outbox/processor"
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

	db := data.NewDB(cfg)
	defer func() {
		if err := db.Close(); err != nil {
			log.Logger.WithError(err).Error("error closing database during shutdown process")
		}
	}()

	repo := outbox.NewRepository(db, cfg)

	switch {
	case cfg.RunCleanup:
		os.Exit(job.RunCleanup(repo, cfg))
	case cfg.RunOptimize:
		os.Exit(job.RunOptimize(db, cfg))
	default:
		data.MigrateDatabase(db, cfg)
		cleanup := startRelayServicePolling(cfg, repo, ctx)
		defer cleanup()

		go prometheus.ObserveQueueSize(repo, ctx)
		go prometheus.ObserveTotalSize(repo, ctx)
		prometheus.StartHttpServer(cfg, db)
	}
}

// todo: move to package
func startRelayServicePolling(cfg *config.Config, repo outbox.Repository, ctx context.Context) func() {
	logger := log.Logger.WithField("config", cfg)

	// if we are in dummy mode then we should not start polling, instead we should
	// wait forever by receiving on a channel that never gets a value, it does not
	// matter that we do not act on context cancellation either as we are not
	// processing anything, and it should be fine to terminate at any point
	if cfg.InDummyMode() {
		logger.Info("starting outbox relay in dummy mode, not polling")
		<-make(chan struct{})
		return func() {}
	}

	logger.Info("starting outbox relay polling")

	batchCh := make(chan *outbox.Batch, 10)
	pub := kafka.NewPublisher(cfg.KafkaHost, kafka.NewSaramaConfig(cfg.TLSEnable, cfg.TLSSkipVerifyPeer))
	go poller.New(repo, batchCh, ctx).Poll(cfg.GetPollIntervalDurationInMs())

	proc := processor.NewBatchProcessor(repo, pub)
	for i := 0; i < cfg.WriteConcurrency; i++ {
		go proc.ListenAndProcess(ctx, batchCh)
	}

	return func() {
		err := pub.Close()
		if err != nil {
			log.Logger.WithError(err).Error("error closing kafka publisher during shutdown")
		}
	}
}
