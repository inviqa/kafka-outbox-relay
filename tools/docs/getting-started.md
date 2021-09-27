# Getting Started

This service provides a reusable docker image at `quay.io/inviqa_images/kafka-outbox-relay`, which can be deployed on your project. See [backwards compatibility](backwards-compatibility.md) for more information on available image tags.

## Using this service

>_NOTE: These docs need enriching_

If you want to use this service to publish messages to Kafka from your application, then the following steps are needed:

1. Familiarise yourself with the [defined schema](/tools/docs/outbox-schema.md) of the outbox table.
2. Ensure that any events triggered by a database query in your application are issued in a transaction, and write a record to this new table containing the following columns:
  * `topic`
  * `payload_json`
  * `key` and `partition_key` (both optional, see [message keys](/tools/docs/message-keys.md))
4. Insert a record in the `kafka_outbox` table with the desired event payload for Kafka, inside that same transaction. This will give us ACID compliance for both the event **and** the original data changes in your application.
5. Deploy this service, configured for your application's database (see configuration below)

>_NOTE: This outbox relay is designed to run for a single application. For example, if you run two different applications that both produce messages in an outbox table, then you will need 2 deployments of this service, each one configured accordingly._

### Testing your application

It is important to note that, when developing your application that publishes events via the outbox table, it is not necessary to run this outbox relay and a Kafka cluster. In local development environments, your application's boundary stops at the outbox table, which will make testing a lot easier as it is only a case of verifying that the outbox record was produced as expected.

### Configuration

See [configuration](configuration.md) for more information.

### Backwards compatibility

See [here](backwards-compatibility.md) for more information.

## Other notes

1. The outbox relay will take care of handling retries when publishing to Kafka.
1. The outbox relay will clean up old outbox message records when they have been successfully published to Kafka more than 1 hour ago. This will be handled by a cron job within the relay service.
1. Your application should not push anything directly to Kafka, all system events should be saved in the outbox table.
