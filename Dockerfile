# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM python:3.6-slim-jessie
LABEL maintainer="Puckel_"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.1

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

ENV AIRFLOW_HOME /airflow
ENV PACKAGE_HOME /project
ENV PYTHONPATH "${PYTHONPATH}:${PACKAGE_HOME}"

RUN set -ex \
    && buildDeps=' \
        python3-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
    ' \
    && apt-get update -y \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        $buildDeps \
        python3-pip \
        python3-requests \
        python-mysqldb \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
        procps \
        vim \
        git \
        crudini \
        mysql-client \
        supervisor \
        libmysqlclient-dev \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

WORKDIR /setup/
COPY requirements.txt /setup
RUN export SLUGIFY_USES_TEXT_UNIDECODE=yes && pip install -r requirements.txt
EXPOSE 8080 5555 8793
RUN chown -R airflow:airflow /airflow
USER airflow
ENTRYPOINT ["/usr/bin/supervisord", "-c", "/project/supervisord.conf"]
# ENTRYPOINT ["/entrypoint.sh"]
# CMD ["webserver"] # set default arg for entrypoint

