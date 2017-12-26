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


echo check that pg_config exists
command -v pg_config


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


# remove existing coverage file
export COVERAGE_FILE=.coverage
rm -f $COVERAGE_FILE


# run tests (PATH)
time coverage run -a tests/test_simple.py


# run tests (PG_BIN)
export PG_BIN=$(dirname $(which pg_config))
time coverage run -a tests/test_simple.py
unset PG_BIN


# run tests (PG_CONFIG)
export PG_CONFIG=$(which pg_config)
time coverage run -a tests/test_simple.py
unset PG_CONFIG


# show coverage
coverage report


# attempt to fix codecov
set +eux

# send coverage stats to Codecov
bash <(curl -s https://codecov.io/bash)
