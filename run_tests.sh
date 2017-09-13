#!/usr/bin/env bash

set -eux

if [ "$PYTHON_VERSION" -eq "2" ]; then
	virtualenv="virtualenv --python=/usr/bin/python2"
	pip=pip2
else
	virtualenv="virtualenv --python=/usr/bin/python3"
	pip=pip3
fi

# prepare environment
$virtualenv env
source env/bin/activate

# install utilities
$pip install coverage codecov flake8

# install testgres
$pip install -U .

# test code quality
flake8 .

# run tests
cd testgres/tests
coverage run test_simple.py

# show coverage
coverage report

# gather reports
codecov
