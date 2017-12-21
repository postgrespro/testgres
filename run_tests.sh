#!/usr/bin/env bash

# Copyright (c) 2017, Postgres Professional

set -eux

if [ "$PYTHON" == "python2" ]; then
	VIRTUALENV="virtualenv --python=/usr/bin/python2"
	PIP=pip2
else
	VIRTUALENV="virtualenv --python=/usr/bin/python3"
	PIP=pip3
fi

# prepare environment
VENV_PATH=/tmp/testgres_venv
rm -rf $VENV_PATH
$VIRTUALENV $VENV_PATH
export VIRTUAL_ENV_DISABLE_PROMPT=1
source $VENV_PATH/bin/activate

# install utilities
$PIP install coverage flake8

# install testgres' dependencies
export PYTHONPATH=$(pwd)
$PIP install .

# test code quality
flake8 .

# run tests
coverage run tests/test_simple.py

# show coverage
coverage report

# attempt to fix codecov
set +eux

# send coverage stats to Codecov
bash <(curl -s https://codecov.io/bash)
