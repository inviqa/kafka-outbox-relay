package poller

import (
	"context"

	"inviqa/kafka-outbox-relay/config"
	"inviqa/kafka-outbox-relay/kafka"
	"inviqa/kafka-outbox-relay/log"
	"inviqa/kafka-outbox-relay/outbox"
	"inviqa/kafka-outbox-relay/outbox/processor"
)

func Start(cfg *config.Config, repo outbox.Repository, ctx context.Context) func() {
	logger := log.Logger.WithField("config", cfg)

	// if polling has been disabled, we should
	// wait forever by receiving on a channel that never gets a value, it does not
	// matter that we do not act on context cancellation either as we are not
	// processing anything, and it should be fine to terminate at any point
	if cfg.PollingDisabled {
		logger.Info("starting outbox relay in simulate mode, not polling")
		<-make(chan struct{})
		return func() {}
	}

	logger.Info("starting outbox relay polling")

	batchCh := make(chan *outbox.Batch, 10)
	pub := kafka.NewPublisher(cfg.KafkaHost, kafka.NewSaramaConfig(cfg.TLSEnable, cfg.TLSSkipVerifyPeer))
	go New(repo, batchCh).Poll(ctx, cfg.GetPollIntervalDurationInMs())

	proc := processor.NewBatchProcessor(repo, pub)
	for i := 0; i < cfg.WriteConcurrency; i++ {
		go proc.ListenAndProcess(ctx, batchCh)
	}

	return func() {
		if err := pub.Close(); err != nil {
			log.Logger.WithError(err).Error("error closing kafka publisher during shutdown")
		}
	}
}
