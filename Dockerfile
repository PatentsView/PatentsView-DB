# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM jupyter/datascience-notebook
USER root

ARG GID=1000
# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.1
ARG GID=1000
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
    '
RUN apt-get update --fix-missing \
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
        openssh-client \
	default-jre \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8

RUN groupadd --gid=$GID docker-group || :
RUN usermod -a -G $GID $NB_USER
RUN apt-get install -y net-tools iputils-ping
RUN apt-get install -y libmysqlclient-dev
RUN wget https://github.com/maxbube/mydumper/releases/download/v0.9.3/mydumper_0.9.3-41.stretch_amd64.deb
RUN dpkg -i mydumper_0.9.3-41.stretch_amd64.deb
USER $NB_USER

RUN pip install https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tarball/master yapf
RUN pip install autopep8 pymysql nltk pymongo google-cloud-pubsub python-dateutil pycryptodome
RUN jupyter contrib nbextension install --user
RUN jupyter nbextension enable code_prettify/code_prettify
RUN jupyter nbextension enable toggle_all_line_numbers/main
RUN jupyter nbextension enable varInspector/main
RUN jupyter nbextension enable code_prettify/2to3
RUN jupyter nbextension enable autosavetime/main
RUN jupyter nbextension enable execute_time/ExecuteTime
RUN jupyter nbextension enable table_beautifier/main
RUN jupyter nbextension enable zenmode/main
RUN jupyter nbextension enable init_cell/main
RUN jupyter nbextension enable table_beautifier/main
RUN jupyter nbextension enable code_font_size/code_font_size
RUN jupyter nbextension enable highlight_selected_word/main
RUN jupyter nbextension enable tree-filter/index
RUN jupyter nbextension enable runtools/main
RUN jupyter nbextension enable snippets/main
RUN jupyter nbextension enable autoscroll/main
WORKDIR /setup/
COPY requirements.txt /setup
RUN export SLUGIFY_USES_TEXT_UNIDECODE=yes && pip install -r /setup/requirements.txt

EXPOSE 8080 5555 8793


ENV PYTHONPATH "${PYTHONPATH}:${PACKAGE_HOME}/Development"
#RUN chown -R airflow:airflow /airflow

WORKDIR /project
ENTRYPOINT ["/usr/bin/supervisord", "-c", "/project/supervisord.conf"]
# ENTRYPOINT ["/entrypoint.sh"]
# CMD ["webserver"] # set default arg for entrypoint

