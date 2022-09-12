#!/usr/bin/env bash

# Copyright (c) 2017, Postgres Professional

set -eux


# choose python version
echo python version is $PYTHON_VERSION
VIRTUALENV="virtualenv --python=/usr/bin/python$PYTHON_VERSION"
PIP="pip$PYTHON_VERSION"

# fail early
echo check that pg_config is in PATH
command -v pg_config

# prepare environment
VENV_PATH=/tmp/testgres_venv
rm -rf $VENV_PATH
$VIRTUALENV $VENV_PATH
export VIRTUAL_ENV_DISABLE_PROMPT=1
source $VENV_PATH/bin/activate

# install utilities
$PIP install coverage flake8 psutil Sphinx sphinxcontrib-napoleon

# install testgres' dependencies
export PYTHONPATH=$(pwd)
$PIP install .
# test code quality
flake8 --ignore=F401,E501,F403 .


# remove existing coverage file
export COVERAGE_FILE=.coverage
rm -f $COVERAGE_FILE


# run tests (PATH)
time coverage run -a tests/test_simple.py


# run tests (PG_BIN)
time \
	PG_BIN=$(dirname $(which pg_config)) \
	ALT_CONFIG=1 \
	coverage run -a tests/test_simple.py


# run tests (PG_CONFIG)
time \
	PG_CONFIG=$(which pg_config) \
	ALT_CONFIG=1 \
	coverage run -a tests/test_simple.py


# show coverage
coverage report

# build documentation
cd docs
make html
cd ..

# attempt to fix codecov
set +eux

# send coverage stats to Codecov
bash <(curl -s https://codecov.io/bash)
bash
