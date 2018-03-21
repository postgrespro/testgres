#!/bin/bash
DIR=$(dirname $0)
ln -s -f ../../hooks/pre-commit "$DIR/../.git/hooks/"
