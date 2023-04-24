#!/usr/bin/env bash

# Copyright (c) 2017-2022 Postgres Professional

set -eux

# prepare environment
venv_path=.venv
rm -rf "$venv_path"
virtualenv "$venv_path"
export VIRTUAL_ENV_DISABLE_PROMPT=1
. "$venv_path"/bin/activate

# install utilities
pip3 install setuptools twine

# create distribution of the package
python3 setup.py sdist bdist_wheel

# upload dist
twine upload dist/*

set +eux
