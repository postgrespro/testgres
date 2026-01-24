#!/usr/bin/env bash

set -eux

eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"

pyenv virtualenv --force ${PYTHON_VERSION} cur
pyenv activate cur

./run_tests.sh
