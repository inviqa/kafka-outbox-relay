CREATE INDEX outbox_batch_query ON kafka_outbox(batch_id, created_at);
