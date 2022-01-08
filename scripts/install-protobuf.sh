#!/bin/sh
set -ex
if [ ! -f "$HOME/bin/protoc" ]; then
  cd $HOME
  wget https://github.com/google/protobuf/releases/download/v3.16.1/protoc-3.16.1-linux-x86_64.zip
  unzip protoc-3.16.1-linux-x86_64.zip
else
  echo "Using cached directory."
fi
