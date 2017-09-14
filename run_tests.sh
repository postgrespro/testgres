#!/usr/bin/env bash

# Copyright (c) 2017, Postgres Professional

set -eux

if [ "$PYTHON" == "python2" ]; then
	virtualenv="virtualenv --python=/usr/bin/python2"
	pip=pip2
else
	virtualenv="virtualenv --python=/usr/bin/python3"
	pip=pip3
fi

# prepare environment
cd ..
$virtualenv env
export VIRTUAL_ENV_DISABLE_PROMPT=1
source env/bin/activate
cd -

# install utilities
$pip install coverage flake8

# install testgres
$pip install .

# test code quality
flake8 .

# run tests
cd testgres/tests
coverage run test_simple.py

# show coverage
coverage report

# send coverage stats to Codecov
bash <(curl -s https://codecov.io/bash)
