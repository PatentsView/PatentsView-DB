FROM apache/airflow
USER root

ARG GID=1000
# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux
ENV PYTHONOPTIMIZE 1

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

ENV AIRFLOW_HOME /airflow
ENV PACKAGE_HOME /project
ENV PYTHONPATH "${PYTHONPATH}:${PACKAGE_HOME}"
ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"
ENV PYTHONOPTIMIZE=1 

#RUN set -ex \
#    && buildDeps='
#        python3-dev \
#        libkrb5-dev \
#        libsasl2-dev \
#        libssl-dev \
#        libffi-dev \
#        build-essential \
#        libblas-dev \
#        liblapack-dev \
#        libpq-dev \
#    '

RUN apt-get update --fix-missing \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
#        $buildDeps \
        python3-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
        python3-pip \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
        procps \
        vim \
        git \
        crudini \
        supervisor \
        openssh-client \
        libsasl2-dev \
	    default-jre \
        g++ \
        default-mysql-client \
        default-libmysqlclient-dev \
        net-tools \
        iputils-ping \
        libffi-dev \
        wget \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8

 
RUN apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid=$GID docker-group || :
#RUN wget https://github.com/maxbube/mydumper/releases/download/v0.9.3/mydumper_0.9.3-41.stretch_amd64.deb
#RUN dpkg -i mydumper_0.9.3-41.stretch_amd64.deb
WORKDIR /setup/

COPY requirements.txt /setup
COPY updater/disambiguation/hierarchical_clustering_disambiguation/requirements.txt /setup/disambig_requirements.txt

USER airflow

RUN export SLUGIFY_USES_TEXT_UNIDECODE=yes && pip install -r /setup/requirements.txt && pip install -r /setup/disambig_requirements.txt && pip install git+https://github.com/PatentsView/grinch.git && pip install git+https://github.com/epfml/sent2vec.git && python -m nltk.downloader stopwords && python -m nltk.downloader punkt && pip install gdown
RUN pip install https://github.com/ipython-contrib/jupyter_contrib_nbextensions/tarball/master yapf
RUN pip install autopep8 pymysql nltk pymongo google-cloud-pubsub python-dateutil pycryptodome


ENV PYTHONPATH "${PYTHONPATH}:${PACKAGE_HOME}/airflow/:${PACKAGE_HOME}/updater/disambiguation/hierarchical_clustering_disambiguation"
ENTRYPOINT ["/usr/bin/supervisord", "-c", "/project/supervisord.conf"]
