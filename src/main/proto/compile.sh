#!/bin/bash

set -o errexit
set -o nounset

PROTOC=`which protoc`

[[ ! -x ${PROTOC} ]] && echo "protoc is not on path or is not executable" && exit 1 || true

SRC_DIR=`dirname ${0}`
DST_DIR="${SRC_DIR}/../../main/java/"

${PROTOC} -I=${SRC_DIR} --java_out=${DST_DIR} ${SRC_DIR}/riemann.proto
