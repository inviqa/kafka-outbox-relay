ALTER TABLE kafka_outbox ADD COLUMN `partition_key` VARCHAR(255) NOT NULL DEFAULT '';
ALTER TABLE kafka_outbox ADD COLUMN `key` VARCHAR(255) NOT NULL DEFAULT '';
