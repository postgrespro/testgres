#!/usr/bin/env bash

set -eux

if [ -z ${TEST_FILTER+x} ]; \
then export TEST_FILTER="TestTestgresLocal or (TestTestgresCommon and (not remote))"; \
fi

echo NPROC: $(nproc)

# fail early
echo check that pg_config is in PATH
command -v pg_config

# prepare python environment
VENV_PATH="/tmp/testgres_venv"
rm -rf $VENV_PATH
${PYTHON_BINARY} -m venv "${VENV_PATH}"
export VIRTUAL_ENV_DISABLE_PROMPT=1
source "${VENV_PATH}/bin/activate"
pip install --upgrade pip setuptools wheel
pip install -r tests/requirements.txt

# remove existing coverage file
export COVERAGE_FILE=.coverage
rm -f $COVERAGE_FILE

pip install coverage

if [ -n "${TEST_CFG__REMOTE_HOST:-}" ] && [ -n "${TEST_CFG__REMOTE_USERNAME:-}" ]; then
    cmd_str="ssh"

    if [ -n "${TEST_CFG__REMOTE_PASSWORD:-}" ]; then
        cmd_str="sshpass -p \"$TEST_CFG__REMOTE_PASSWORD\" $cmd_str"
    fi

    [ -n "${TEST_CFG__REMOTE_SSH_KEY:-}" ] && cmd_str="$cmd_str -i \"$TEST_CFG__REMOTE_SSH_KEY\""
    [ -n "${TEST_CFG__REMOTE_PORT:-}" ]    && cmd_str="$cmd_str -p \"$TEST_CFG__REMOTE_PORT\""

    cmd_str="$cmd_str \"$TEST_CFG__REMOTE_USERNAME@$TEST_CFG__REMOTE_HOST\" 'df -T'"
    REMOTE_FS_STATE_QUERY="$cmd_str"
else
    REMOTE_FS_STATE_QUERY=""
fi

show_fs_state() {
    df -T
    if [ -n "$REMOTE_FS_STATE_QUERY" ]; then
        eval "$REMOTE_FS_STATE_QUERY"
    fi
}

# ---------------------------------------- PATH

show_fs_state

# run tests (PATH)
time coverage run -a -m pytest -l -vvv -n 8 -k "${TEST_FILTER}"

# ---------------------------------------- PG_BIN

show_fs_state

# run tests (PG_BIN)
PG_BIN=$(pg_config --bindir) \
time coverage run -a -m pytest -l -vvv -n 8 -k "${TEST_FILTER}"

# ---------------------------------------- PG_CONFIG

show_fs_state

# run tests (PG_CONFIG)
PG_CONFIG=$(pg_config --bindir)/pg_config \
time coverage run -a -m pytest -l -vvv -n 8 -k "${TEST_FILTER}"

# ---------------------------------------- pg8000

show_fs_state

# test pg8000
pip uninstall -y psycopg2
pip install pg8000
PG_CONFIG=$(pg_config --bindir)/pg_config \
time coverage run -a -m pytest -l -vvv -n 8 -k "${TEST_FILTER}"

# ---------------------------------------- finish

show_fs_state

# ---------------------------------------- coverage

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
