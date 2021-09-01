# Message keys

Each message added to the outbox table can have a message key. In Kafka, the message key is used to provide predictability in terms of the partition that a message is directed to when the producer sends it to the cluster. It also plays a crucial role in [log compaction], where Kafka will retain at least the most recent message for each message key. 

## How we have modelled message keys

To give the most flexibility possible, the outbox relay [DB schema] provides two attributes for keys.

1. `key`
2. `partition_key`

We will take a look at these in more detail below.

### `key`

In order to make use of keys you must provide **at least** this attribute value in your outbox record. When populated, this value is sent along with your message payload and topic name to Kafka, and the broker will then store it for use in [log compaction], if it is enabled. Additionally, it is used when determining which partition your message will be sent to. See [below](#partitioning-using-the-key) for more info.

### `partition_key`

When using keys, this is an optional extra field for using a different key value than the one set in the `key` attribute when determining what partition your message should be sent to. It is **not** sent to Kafka as the message key, the `key` attribute will **always** be the value sent as the message key to Kafka. This is useful when you have a single topic that stores different kinds of messages for multiple different entities. See [the section below](#partitioning-using-the-key) for an example and explanation of why this extra flexibility is needed.

## Partitioning using the key

Kafka only guarantees the order of messages within a single topic partition. When we start to add more partitions to our topics, we will want messages related to the same entity to probably go to the same partition, so if we ever replay events from the topic we end up in a consistent state in our consuming system at the end.

Let's consider the example of product update and delete events sent to a single `event.product` topic. We want to use the product's SKU when determining what partition the message should be sent to, so that order of events for the same entity is guaranteed. Additionally, if [log compaction] runs in the Kafka broker then we want to make sure that we retain at least the latest product update **and** product delete event to avoid any important event data loss. To do this, we decide to include a prefix in the `key` to indicate whether it was a delete event or an update event, so we end up with outbox records like this:

1. `productUpdated` event for product with identifier of SKU-123: `<key: "UPDATE-SKU-123", partition_key:"SKU-123">`
1. `productDeleted` event for product with identifier of SKU-123: `<key: "DELETE-SKU-123", partition_key:"SKU-123">`

The outcome here is two-fold:

1. All events for a product with identifier of SKU-123 are sent to the same topic partition, because the `partition_key` is always the same for that product
2. The `key` value sent to Kafka includes the event type (e.g. `DELETE`, or `UPDATE`) so that when log compaction runs we are left with the latest update event and delete event for each product.

## What key-related attributes should I set in my outbox record?

Unless you have log compaction enabled you will only ever need to set the `key` attribute value. Having said that, even if you do have log compaction enabled you may decide to make your topics more specific, e.g. keeping all `productDeleted` events in their own topic. If this is the case, then you would never need to set a special `partition_key` value. However, the option is there to provide ultimate flexibility.

[log compaction]: https://kafka.apache.org/documentation.html#compaction
[DB Schema]: /tools/docs/outbox-schema.md
