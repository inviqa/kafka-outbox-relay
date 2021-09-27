# Upgrades

This document highlights breaking changes made in major versions of the relay image. See [backwards compatibility](/tools/docs/backwards-compatibility.md) for more information.

## `v0` -> `v1`

* The outbox relay is now responsible for managing its own schema within your application's database.
* Your application's outbox table **must** be called `kafka_outbox`, otherwise the outbox relay will create it when it is next deployed.
* The `DB_SCHEMA` configuration env var has been renamed to `DB_NAME`, so you will need to change this in your environment configuration.
