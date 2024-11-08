#!/bin/bash

set -ex

./scripts/build/package.sh
docker pull openjdk:11
docker build -f ./Dockerfile -t docker.oa.com:5000/mpc/openhufu-server:1.0 .