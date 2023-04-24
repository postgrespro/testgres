#!/usr/bin/env bash

# Copyright (c) 2017-2022 Postgres Professional

set -eu

venv_path=.venv
rm -rf "$venv_path"
virtualenv "$venv_path"
export VIRTUAL_ENV_DISABLE_PROMPT=1
. "$venv_path"/bin/activate

pip3 install setuptools twine
python3 setup.py sdist bdist_wheel
twine upload dist/*

