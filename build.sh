#!/bin/bash

set -e

# build bin file
make build

# make my dir
mv cmd/router/router $BUILD_ROOT