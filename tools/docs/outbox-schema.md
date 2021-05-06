# Outbox Schema

In each application that wants to use the outbox relay service, there needs to be a consistent DB schema available for it to fetch messages from. This schema is defined below.

## Schema

| Column            | Type                    | App value required? | Description                                                             |
|-------------------|-------------------------|---------------------|-------------------------------------------------------------------------|
| id                | bigint, unsigned        | yes                 | Primary key, auto-incrementing                                          |
| batch_id          | string, nullable        | no                  | The batch ID (UUID) used internally by the outbox relay.                |
| push_started_at   | datetime, nullable      | no                  | When the push to Kafka was started for this message                     |
| push_completed_at | datetime, nullable      | no                  | When the push to Kafka was completed for this message                   |
| topic             | string                  | yes                 | The topic to publish this message to in Kafka                           |
| payload_json      | text                    | yes                 | The raw JSON payload to send to Kafka                                   |
| payload_headers   | text                    | no                  | JSON serialized representation of the payload headers to send to Kafka. |
| push_attempts     | int                     | no, default: 0      | Number of attempts so far trying to push this message to Kafka.         |
| errored           | int                     | no, default: 0      | If the message has exceeded the maximum push_attempts, this will be 1   |
| error_reason      | string                  | no                  | The reason for the last error on this message                           |
| created_at        | datetime                | yes                 | When this record was created                                            |

### Required values

In the schema description above, columns where a value is required from the application creating the outbox record, are noted accordingly. The other columns are managed solely by the outbox relay service, and should not be processed by the application.

### Indexes

Aside from the primary key, there should be indexes placed on the following columns, to improve performance of outbox relay service:

* `push_completed_at`, non-unique (used by the relay service to determine the number of messages pending publish)
