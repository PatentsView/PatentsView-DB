FROM patentsview/airflow:0.5
USER root

# Add MySQL repo key into a keyring and (re)declare the source with signed-by
RUN set -eux; \
    install -d -m 0755 /usr/share/keyrings; \
    wget -qO- https://repo.mysql.com/RPM-GPG-KEY-mysql-2023 | gpg --dearmor -o /usr/share/keyrings/mysql-archive-keyring.gpg; \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/mysql-archive-keyring.gpg] http://repo.mysql.com/apt/debian bullseye mysql-apt-config mysql-8.0 mysql-tools" > /etc/apt/sources.list.d/mysql.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends libglib2.0-0 wget; \
    rm -rf /var/lib/apt/lists/*

# mydumper install
RUN set -eux; \
    cd /tmp; \
    wget -q https://github.com/maxbube/mydumper/releases/download/v0.9.3/mydumper_0.9.3-41.stretch_amd64.deb; \
    dpkg -i mydumper_0.9.3-41.stretch_amd64.deb || apt-get -f install -y; \
    rm -f mydumper_0.9.3-41.stretch_amd64.deb

COPY mydumper.cnf /etc/mydumper.cnf
USER airflow
CMD ["mydumper", "--version"]

