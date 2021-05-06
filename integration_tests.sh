#!/usr/bin/env bash

if [[ "$(docker-compose ps -q kafka)" != "" ]];
then
  docker-compose stop app && docker-compose rm -f app && ws harness prepare && ws build && docker-compose up -d app
else
  ws destroy && ws harness prepare && ws build && ws install
  echo "waiting for stack..."
  sleep 10
fi

ws go test integration mysql
ws go test integration postgres
