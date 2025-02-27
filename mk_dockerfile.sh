set -eu
sed -e 's/${PYTHON_VERSION}/'${PYTHON_VERSION}/g -e 's/${PG_VERSION}/'${PG_VERSION}/g Dockerfile--${TEST_PLATFORM}.tmpl > Dockerfile
