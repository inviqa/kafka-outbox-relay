# Outbox Schema

In each application that wants to use the outbox relay service, there needs to be a consistent DB schema available for it to fetch messages from. This schema is defined below.

## Schema

The schema is managed purely by the outbox relay service. Whilst it sits inside your main application's database, it is managed by the relay to make it easier when upgrading to newer version of the outbox relay.

The outbox table name is `kafka_outbox`.

### Public API

It is critical that your application only relies upon the columns marked as being part of the public API in the table below. The only interaction your application has with the outbox table should be the creation of new records, in which only the public API fields matter, the rest can be omitted from your `INSERT` statement. If you must run other queries against this table, then they should only use the public API fields.

>_NOTE: If you do query any of the non-public API columns in your application code, do so at your own risk. **No** guarantees are made for fields other than those that are part of the public API, and they may be renamed or removed in the future._

| Column            | Type               | App value required?  | Public API? | Description                                                                                                       |
|-------------------|--------------------|----------------------|-------------|-------------------------------------------------------------------------------------------------------------------|
| id                | bigint, unsigned   | no                   | yes         | Primary key, auto-incrementing                                                                                    |
| batch_id          | string, nullable   | no                   | no          | The batch ID (UUID) used internally by the outbox relay.                                                          |
| push_started_at   | datetime, nullable | no                   | no          | When the push to Kafka was started for this message                                                               |
| push_completed_at | datetime, nullable | no                   | no          | When the push to Kafka was completed for this message                                                             |
| topic             | string             | yes                  | yes         | The topic to publish this message to in Kafka                                                                     |
| payload_json      | text               | yes                  | yes         | The raw JSON payload to send to Kafka                                                                             |
| payload_headers   | text               | no, default: ''      | yes         | JSON serialized representation of the payload headers to send to Kafka.                                           |
| push_attempts     | int                | no, default: 0       | no          | Number of attempts so far trying to push this message to Kafka.                                                   |
| key               | string             | no, default: ''      | yes         | The message key stored in produced Kafka message.                                                                 |
| partition_key     | string             | no, default: ''      | yes         | The key used when determining which partition the message should be sent to. If empty, then "key" is used instead |
| errored           | int                | no, default: 0       | no          | If the message has exceeded the maximum push_attempts, this will be 1                                             |
| error_reason      | string             | no, default: ''      | no          | The reason for the last error on this message                                                                     |
| created_at        | datetime           | no, default: `now()` | no          | When this record was created                                                                                      |

### Required values

In the schema description above, columns where a value is required from the application creating the outbox record, are noted accordingly. The other columns are managed solely by the outbox relay service, and should not be processed by the application.

### Indexes

Aside from the primary key, there are indexes placed on the following columns, to improve performance of outbox relay service:

* `push_completed_at`, non-unique (used by the relay service to determine the number of messages pending publish)
