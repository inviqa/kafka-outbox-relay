# Backwards Compatibility

The public API surface area of the outbox relay is fairly small, however, it still provides backwards compatibility guarantees within the same major version of the relay. When pulling the docker image for the relay service, you should select a major version tag from below:

* `v1`

## Major versions

A major version will change over time, and receive new features and bugfixes. However, within each major version we will not break backwards compatibility, so you only need to consult the [upgrade](/UPGRADE.md) file when you are switching to a new major version to see what has breaking changes have been made. These will usually mean that you need to change how you use the relay.

>_NOTE: Using a major version tag (e.g. `v1`) is the recommended way to pull the outbox relay image into your project._

## Pre-`v1`

If you are coming from pre-`v1`, then it is recommended to ask in the [#distributed-architecture](https://inviqa.slack.com/archives/C024BMRM4V9) Slack channel for advice on how best to determine what is needed to switch to `v1`, where some breaking changes were made.
