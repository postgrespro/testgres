#!/usr/bin/env bash

set -eux

# DEBUG port-for
cat /proc/sys/net/ipv4/ip_local_port_range
${PYTHON} -c "with open('/proc/sys/net/ipv4/ip_local_port_range') as f: l, h = f.read().split(); print(l); print(h);"

cd testgres/tests
${PYTHON} -m unittest test_simple

cd ../..
flake8 --ignore=W191,F401,E501,F403 .
