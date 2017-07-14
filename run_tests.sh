#!/usr/bin/env bash

# DEBUG port-for
cat /proc/sys/net/ipv4/ip_local_port_range

cd testgres/tests
${PYTHON} -m unittest test_simple
flake8 --ignore=W191,F401,E501,F403 .
