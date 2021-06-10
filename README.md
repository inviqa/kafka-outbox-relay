# Kafka Outbox Relay

A common relay service for publishing messages stored in systems with a DB-based message outbox, to Kafka.

## Development Environment

### Getting Started

#### Prerequisites

##### General

##### Docker

- A working Docker setup
  - On MacOS, use [Docker for Mac](https://docs.docker.com/docker-for-mac/install/).
  - On Linux, add the official Docker repository and install the "docker-ce" package.
    You will also need to have a recent [docker-compose](https://docs.docker.com/compose/install/) version - at least `1.24.0`.

### Setup

#### On workspace

1. Install the latest version of [workspace](https://github.com/my127/workspace)
2. Copy the LastPass entry "kafka-outbox-relay: Development Environment Key" to a file named `workspace.override.yml` in the project root.
3. Run `ws install`

#### Running tests

Tests should be run on your host machine to speed up the feedback cycle. You can run tests with `go test ./...`.

##### Integration tests

To run the integration tests on your host machine, run `ws go test integration`. Please be aware that a running environment is required to run the integration tests from your host machine, because they connect to the database and Kafka broker defined in `docker-compose.yml`.

## Using this service

>_NOTE: These docs need enriching_

If you want to use this service to publish messages to Kafka from your application, then the following steps are needed:

1. Create a new table in your application's database that matches the [defined schema](tools/docs/outbox-schema.md)
1. Ensure that any events triggered by a database query in your application are issued in a transaction, and write a record to this new table containing the desired event payload for Kafka, inside that same transaction. This will give us ACID compliance for both the event **and** the original data changes in your application.
1. Deploy this service, configured for your application's database (see configuration below)

>_NOTE: This outbox relay is designed to run for a single application. For example, if you run two different applications that both produce messages in an outbox table, then you will need 2 deployments of this service, each one configured accordingly._

### Configuration

There are several environment variables available in this service that can be used to configure how it connects and behaves:

| Environment Variable | Description                                                                                                                                                                                                                                                                                                              |
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ENABLE_MIGRATIONS    | Whether to automatically run database migrations in the outbox table. **DO NOT** enable this option as database migrations may unintentionally break your application's ability to store messages in the outbox. Instead, always make sure that your application creating the outbox messages sets up the schema itself. |
| DB_HOST              | The database host where the outbox table resides.                                                                                                                                                                                                                                                                        |
| DB_PORT              | Database port.                                                                                                                                                                                                                                                                                                           |
| DB_USER              | Database user.                                                                                                                                                                                                                                                                                                           |
| DB_PASS              | Database password.                                                                                                                                                                                                                                                                                                       |
| DB_SCHEMA            | Database name.                                                                                                                                                                                                                                                                                                           |
| DB_DRIVER            | The type of database driver to use, options are either "mysql" or "postgres".                                                                                                                                                                                                                                            |
| DB_OUTBOX_TABLE      | The name of the outbox table where messages should be read from.                                                                                                                                                                                                                                                         |
| KAFKA_HOST           | The Kafka host, should be comma separated when there are multiple Kafka brokers, e.g. "kafka1:9092,kafka2:9092"                                                                                                                                                                                                          |
| TLS_ENABLE           | Whether to enable TLS when communicating with Kafka and the database. We recommend enabling this if your database and Kafka cluster support it. Defaults to false.                                                                                                                                                       |
| TLS_SKIP_VERIFY_PEER | Whether to skip peer verification when connecting over TLS. Defaults to false.                                                                                                                                                                                                                                           |
| WRITE_CONCURRENCY    | The number of concurrent workers used to push data to Kafka. Defaults to 1. You should only need to increase this if the throughput of messages to the outbox is extremely high.                                                                                                                                         |
| POLL_FREQUENCY_MS    | How frequently, in milliseconds, to poll the outbox table for new messages. Defaults to 500.                                                                                                                                                                                                                             |
| BATCH_SIZE           | The maximum number of messages to grab from the outbox table for each poll operation. Defaults to 250.                                                                                                                                                                                                                   |

### Running the cleanup job

This service provides a cleanup job that should be executed periodically. It will delete any outbox records that have been successfully published more than 1 hour ago.

To run this job manually, in your dev environment you can run

    $ docker-compose exec app /go/bin/app --cleanup

When deployed, this cron will be executed by Kubernetes' scheduler.

### Running the database optimize job

For both MySQL and Postgres, after records are deleted by the cleanup job above, disk space is not freed for new records. In order to free up the disk space, we need to run an optimization on the database. In MySQL this is `OPTIMIZE`, and in Postgres this is `VACUUM`.

You can run these manually, in your local dev environment:

    $ docker-compose exec app /go/bin/app --optimize

When executed in this way, the application will determine which optimization to run based on the configured database driver.

In pipeline environments that use Kubernetes, you can enable this job by setting `run_optimize: true` at the root level of your Helm chart's `values.yaml` file.

## Other notes

1. The name of the outbox schema table can be anything you like, but when the outbox relay service is deployed the `DB_OUTBOX_TABLE` environment variable must contain that table name.
1. The outbox relay will take care of handling retries when publishing to Kafka.
1. The outbox relay will clean up old outbox message records when they have been successfully published to Kafka more than 24 hours prior. This will be handled by a cron job within the relay service.
1. Your application should not push anything directly to Kafka, all system events should be saved in the outbox schema.

# License

Copyright 2021, Inviqa

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
