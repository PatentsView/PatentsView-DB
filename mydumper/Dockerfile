FROM patentsview/airflow:0.5

USER root

# Disable MySQL APT repo (it’s causing GPG failures), then install mydumper.
# If mydumper isn't in bullseye main, fall back to bullseye-backports.
RUN set -eux; \
    # Comment out any MySQL repo entries (or remove the files if you prefer)
    if ls /etc/apt/sources.list.d/*mysql* >/dev/null 2>&1; then \
        sed -i 's/^[[:space:]]*deb /# deb /' /etc/apt/sources.list.d/*mysql* || true; \
    fi; \
    apt-get update; \
    (apt-get install -y --no-install-recommends mydumper \
     || (echo "deb http://deb.debian.org/debian bullseye-backports main" > /etc/apt/sources.list.d/backports.list \
         && apt-get update \
         && apt-get -y -t bullseye-backports install --no-install-recommends mydumper)); \
    rm -rf /var/lib/apt/lists/*

# Copy config (optional)
COPY mydumper.cnf /etc/mydumper.cnf

# Drop back to runtime user
USER airflow

# (Optional) keep this as a quick sanity check at runtime; your compose command overrides it anyway
CMD ["mydumper", "--version"]

