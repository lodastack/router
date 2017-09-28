#!/bin/bash

set -e

# build bin file
make build

# make my dir
mv cmd/router/router $BUILD_ROOT
mkdir -p $BUILD_ROOT/conf
mv router.conf $BUILD_ROOT/conf