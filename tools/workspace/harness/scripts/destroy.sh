#!/usr/bin/env bash

function destroy() {
  run docker-compose down --rmi local --volumes --remove-orphans

  builder=$(docker buildx inspect | grep Name: | head -n 1 | awk '{print $2}')
  if [[ "$builder" != "" ]]; then
    run docker buildx rm "${builder}"
  fi

  passthru ws cleanup built-images
  run rm -f .my127ws/.flag-built
}

destroy
