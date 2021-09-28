# Getting Started

This service provides a reusable docker image at `quay.io/inviqa_images/kafka-outbox-relay`, which can be deployed on your project. See [backwards compatibility](backwards-compatibility.md) for more information on available image tags.

>_NOTE: This outbox relay is designed to run for a single application. For example, if you run two different applications that both produce messages in an outbox table, then you will need 2 deployments of this service, each one configured accordingly._

## Using this service

If you want to use this service to publish messages to Kafka from your application, then we will need to update our application's local development stack, as well as deploy the outbox relay to our pipeline and production environments.

### Local development

In the application that wants to publish outbox records, we need to bring the outbox relay into the local `docker-compose` stack.

An example `docker-compose.yml` block for this may look something like below. The database values should be for our application's database.

```yaml
outbox-relay:
  image: quay.io/inviqa_images/kafka-outbox-relay:v1
  environment:
    DB_HOST: "db-host"
    DB_PORT: 3306
    DB_USER: "db-user"
    DB_PASS: "pass123"
    DB_NAME: "my-app-db"
    DB_DRIVER: "mysql"
    LOG_LEVEL: "info"
    POLLING_DISABLED: "true"
  networks:
    private: {}
```

>_NOTE: The `POLLING_DISABLED` env var is important, because it tells the relay to start up and create the outbox table for us, but not to actually poll for messages. In local development of our application, this helps massively because we do not need to bring in a Kafka broker, and our boundary stops at the outbox table._

You can now start to write events to the outbox table from your application.

1. Familiarise yourself with the [defined schema](outbox-schema.md) of the outbox table.
2. Ensure that any events triggered by a database query in your application are issued in a transaction, and write a record to this new table containing the following columns:
   * `topic`
   * `payload_json`
   * `key` and `partition_key` (both optional, see [message keys](message-keys.md))
4. Insert a record in the `kafka_outbox` table with the desired event payload for Kafka, inside that same transaction. This will give us ACID compliance for both the event **and** the original data changes in your application.

### Pipeline and production environments

You will need to deploy the `quay.io/inviqa_images/kafka-outbox-relay` image, with the correct tag, to your target environment. When deployed, make sure that the `POLLING_DISABLED` env var is removed or set to `false` (this is the default value for this setting).

See [configuration](configuration.md) for a complete list of configuration options that can be set from env vars.

### Testing your application

It is important to note that, when developing your application that publishes events via the outbox table, it is not necessary to run a Kafka cluster. In local development environments, your application's boundary stops at the outbox table, which will make testing a lot easier as it is only a case of verifying that the outbox record was produced as expected.

However, as noted [above](#local-development), you will need to run the outbox relay in your local application stack with the `POLLING_DISABLED` env var set to `true`. This is so that the outbox database table is created for our application to write to.

### Configuration

See [configuration](configuration.md) for more information.

### Backwards compatibility

See [here](backwards-compatibility.md) for more information.

## Other notes

1. The outbox relay will take care of handling retries when publishing to Kafka.
1. The outbox relay will clean up old outbox message records when they have been successfully published to Kafka more than 1 hour ago. This will be handled by a cron job within the relay service.
1. Your application should not push anything directly to Kafka, all system events should be saved in the outbox table.
