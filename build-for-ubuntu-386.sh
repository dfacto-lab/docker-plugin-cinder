#!/usr/bin/env bash

TARGET_PLATFORM=linux
TARGET_ARCH=amd64
TARGET_EXE_NAME=docker-plugin-cinder

echo "Building executable ${TARGET_EXE_NAME}, for PLATFORM: ${TARGET_PLATFORM} and ARCH: ${TARGET_ARCH}"
env GOOS=${TARGET_PLATFORM} GOARCH=${TARGET_ARCH} go build -o bin/${TARGET_EXE_NAME}
echo "Build DONE"
file bin/docker-plugin-cinder
