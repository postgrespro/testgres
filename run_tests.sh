#!/usr/bin/env bash

set -eux

cd testgres/tests
${PYTHON} -m unittest test_simple

cd ../..
flake8 .

cd ../..
