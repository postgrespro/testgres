#!/usr/bin/env bash

# Copyright (c) 2017-2023 Postgres Professional

set -eux

if [ -z ${TEST_FILTER+x} ]; \
then export TEST_FILTER="TestgresTests or (TestTestgresCommon and (not remote_ops))"; \
fi

# choose python version
echo python version is $PYTHON_VERSION
PIP="pip$PYTHON_VERSION"
VIRTUALENV="virtualenv --python=/usr/bin/python$PYTHON_VERSION"

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
$PIP install coverage flake8 psutil Sphinx pytest pytest-xdist psycopg2 six psutil

# install testgres' dependencies
export PYTHONPATH=$(pwd)
# $PIP install .

# test code quality
flake8 .


# remove existing coverage file
export COVERAGE_FILE=.coverage
rm -f $COVERAGE_FILE


# run tests (PATH)
time coverage run -a -m pytest -l -v -n 4 -k "${TEST_FILTER}"


# run tests (PG_BIN)
PG_BIN=$(pg_config --bindir) \
time coverage run -a -m pytest -l -v -n 4 -k "${TEST_FILTER}"


# run tests (PG_CONFIG)
PG_CONFIG=$(pg_config --bindir)/pg_config \
time coverage run -a -m pytest -l -v -n 4 -k "${TEST_FILTER}"


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
