# Kafka Outbox Relay

A common relay service for publishing messages stored in systems with a DB-based message outbox, to Kafka.

## Development Environment

### Getting Started

#### Prerequisites

##### General

Familiarisation of the [outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html).

##### Docker

- A working Docker setup
  - On macOS, use [Docker for Mac](https://docs.docker.com/docker-for-mac/install/).
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

## Documentation

You can read more about how to use this service in the [docs](/tools/docs).

## License

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
