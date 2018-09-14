#!/bin/bash

set -e

# build bin file
export GO111MODULE=on
make build

# make my dir
mv cmd/router/router $BUILD_ROOT
