#!/usr/bin/env bash

# Copyright (c) 2017-2022 Postgres Professional

set -eux


# choose python version
echo python version is $PYTHON_VERSION
VIRTUALENV="virtualenv --python=/usr/bin/python$PYTHON_VERSION"
PIP="pip$PYTHON_VERSION"


# prepare environment
VENV_PATH=/tmp/testgres_venv
rm -rf $VENV_PATH
$VIRTUALENV $VENV_PATH
export VIRTUAL_ENV_DISABLE_PROMPT=1
source $VENV_PATH/bin/activate

# install utilities
$PIP install setuptools twine

# create distribution of the package
alias python3='python'
python setup.py sdist bdist_wheel

# upload dist
twine upload dist/*

set +eux
