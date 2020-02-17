#!/bin/bash

set -eu

echo -e "Installing maven snapshot locally...\n"

version=${1:-0.6.0}

bash $(dirname $0)/execute-deploy.sh \
  "install:install-file" \
  $version

echo -e "Installed local snapshot"
