CREATE TABLE IF NOT EXISTS kafka_outbox(
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    batch_id CHAR(36) NULL,
    push_started_at DATETIME NULL,
    push_completed_at DATETIME NULL,
    topic VARCHAR(255) NOT NULL,
    payload_json json NOT NULL,
    payload_headers json NOT NULL,
    push_attempts TINYINT NOT NULL DEFAULT 0,
    errored TINYINT NOT NULL DEFAULT 0,
    error_reason VARCHAR (255) NOT NULL DEFAULT '',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
