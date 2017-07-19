#!/usr/bin/env bash

set -eux

cd testgres/tests
${PYTHON} -m unittest test_simple

cd ../..
flake8 --ignore=W191,F401,E501,F403 .
