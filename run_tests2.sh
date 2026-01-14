#!/usr/bin/env bash

set -eux

eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

pyenv virtualenv --force ${PYTHON_VERSION} cur
pyenv activate cur

if [ -z ${TEST_FILTER+x} ]; \
then export TEST_FILTER="TestTestgresLocal or (TestTestgresCommon and (not remote))"; \
fi

# fail early
echo check that pg_config is in PATH
command -v pg_config

# prepare python environment
VENV_PATH="/tmp/testgres_venv"
rm -rf $VENV_PATH
python -m venv "${VENV_PATH}"
export VIRTUAL_ENV_DISABLE_PROMPT=1
source "${VENV_PATH}/bin/activate"
pip install -r tests/requirements.txt

# test code quality
pip install flake8 flake8-pyproject
flake8 .
pip uninstall -y flake8 flake8-pyproject

# remove existing coverage file
export COVERAGE_FILE=.coverage
rm -f $COVERAGE_FILE

pip install coverage

# run tests (PATH)
time coverage run -a -m pytest -l -v -n 4 -k "${TEST_FILTER}"

# run tests (PG_BIN)
PG_BIN=$(pg_config --bindir) \
time coverage run -a -m pytest -l -v -n 4 -k "${TEST_FILTER}"

# run tests (PG_CONFIG)
PG_CONFIG=$(pg_config --bindir)/pg_config \
time coverage run -a -m pytest -l -v -n 4 -k "${TEST_FILTER}"

# test pg8000
pip uninstall -y psycopg2
pip install pg8000
PG_CONFIG=$(pg_config --bindir)/pg_config \
time coverage run -a -m pytest -l -v -n 4 -k "${TEST_FILTER}"

# show coverage
coverage report

pip uninstall -y coverage

# build documentation
pip install Sphinx

cd docs
make html
cd ..

pip uninstall -y Sphinx

# attempt to fix codecov
set +eux

# send coverage stats to Codecov
bash <(curl -s https://codecov.io/bash)
