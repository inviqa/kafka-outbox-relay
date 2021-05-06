CREATE TABLE IF NOT EXISTS kafka_outbox(
    id SERIAL PRIMARY KEY,
    batch_id CHAR(36) NULL,
    push_started_at timestamp NULL,
    push_completed_at timestamp NULL,
    topic VARCHAR(255) NOT NULL,
    payload_json json NOT NULL,
    payload_headers json NOT NULL,
    push_attempts smallint NOT NULL DEFAULT 0,
    errored smallint NOT NULL DEFAULT 0,
    error_reason VARCHAR (255) NOT NULL DEFAULT '',
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);
