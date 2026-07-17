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

    REMOTE_SSH_PREFIX="$cmd_str \"$TEST_CFG__REMOTE_USERNAME@$TEST_CFG__REMOTE_HOST\""
else
    REMOTE_SSH_PREFIX=""
fi

exec_command() {
    local cmd="$1"
    local prefix="$2"

    eval "$prefix $cmd"
}

show_fs_state__impl() {
    local prefix="$1"
    local host_label="$2"

    set +x
    echo "------------- ${host_label} FS STATE"
    set -x
    exec_command "df -T" "$prefix"
}

check_leftover_ports__impl() {
    local prefix="$1"
    local host_label="$2"
    local ports_dir="/tmp/testgres/ports"

    set +x
    echo "------------- Checking ${host_label} ports lock directory"
    set -x

    # Check command: will print FOUND if the directory exists and is not empty
    local check_cmd="if [ -d '${ports_dir}' ] && [ \"\$(ls -A '${ports_dir}' 2>/dev/null)\" ]; then echo 'FOUND'; fi"

    # Temporarily disable bash's instant drop (set +e) to safely intercept the result
    set +e
    local result
    result=$(exec_command "$check_cmd" "$prefix")
    set -e

    set +x
    if [ "$result" = "FOUND" ]; then
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        echo "ERROR: Leftover ports detected in $ports_dir on $host_label machine!"
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        set -x

        # We display a list of frozen ports so that the culprits can be identified
        exec_command "ls -la '$ports_dir'" "$prefix"
        
        # We hard-drop the entire control script
        # sleep 3600
        exit 1
    else
        echo "Clear. No leftover port locks."
    fi
    set -x
}

fs_verification__impl() {
    show_fs_state__impl "$1" "$2"

    check_leftover_ports__impl "$1" "$2"
}

fs_verification() {
    fs_verification__impl "" "LOCAL"
    
    if [ -n "$REMOTE_SSH_PREFIX" ]; then
        fs_verification__impl "$REMOTE_SSH_PREFIX" "REMOTE"
    fi
}

# ---------------------------------------- PATH

fs_verification

# run tests (PATH)
time coverage run -a -m pytest -l -vvv -n 4 -k "${TEST_FILTER}"

# ---------------------------------------- PG_BIN

fs_verification

# run tests (PG_BIN)
PG_BIN=$(pg_config --bindir) \
time coverage run -a -m pytest -l -vvv -n 4 -k "${TEST_FILTER}"

# ---------------------------------------- PG_CONFIG

fs_verification

# run tests (PG_CONFIG)
PG_CONFIG=$(pg_config --bindir)/pg_config \
time coverage run -a -m pytest -l -vvv -n 4 -k "${TEST_FILTER}"

# ---------------------------------------- pg8000

fs_verification

# test pg8000
pip uninstall -y psycopg2
pip install pg8000
PG_CONFIG=$(pg_config --bindir)/pg_config \
time coverage run -a -m pytest -l -vvv -n 4 -k "${TEST_FILTER}"

# ---------------------------------------- finish

fs_verification

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
