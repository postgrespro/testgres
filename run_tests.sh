#!/usr/bin/env bash

cd testgres/tests
${PYTHON} -m unittest test_simple
flake8 --ignore=W191,F401,E501,F403 .
