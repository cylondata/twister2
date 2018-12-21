#!/usr/bin/env bash

if ! type "sass" > /dev/null; then
  echo "sass is required to build the web app. Please install sass and retry"
  exit 0
fi

if ! type "npm" > /dev/null; then
  echo "npm is required to build the web app. Please install npm and retry"
  exit 0
fi

npm i
npm run build-css
npm run build