#!/usr/bin/env bash

WORK_DIR="$(dirname $0)"

# build new docs and copy them to root
make -C "$WORK_DIR" html
cp -R "$WORK_DIR"/build/html/* "$WORK_DIR"/..
