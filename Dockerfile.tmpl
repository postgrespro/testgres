FROM postgres:${PG_VERSION}-alpine

ENV PYTHON=python${PYTHON_VERSION}
RUN if [ "${PYTHON_VERSION}" = "2" ] ; then \
	apk add --no-cache curl python2 python2-dev build-base musl-dev \
    linux-headers py-virtualenv py-pip; \
	fi
RUN if [ "${PYTHON_VERSION}" = "3" ] ; then \
	apk add --no-cache curl python3 python3-dev build-base musl-dev \
    linux-headers py-virtualenv; \
	fi
ENV LANG=C.UTF-8

RUN mkdir -p /pg
COPY run_tests.sh /run.sh
RUN chmod 755 /run.sh

ADD . /pg/testgres
WORKDIR /pg/testgres
RUN chown -R postgres:postgres /pg

USER postgres
ENTRYPOINT PYTHON_VERSION=${PYTHON_VERSION} /run.sh
