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

	"github.com/sirupsen/logrus"
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
	log.Logger.WithFields(logrus.Fields{
		"config": cfg,
	}).Info("starting service")

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
