# Use an official Debian image as the base
FROM debian:bullseye

# Install mydumper and libmariadb3 using package manager
RUN apt-get update && \
    apt-get install -y libmariadb-dev && apt-get install -y libmariadb3 && apt-get install -y libmariadb-dev-compat

# Use the mysqlboy/docker-mydumper image as a base
FROM mydumper/mydumper:latest

# Copy mydumper configuration
COPY mydumper.cnf /etc/

# Set the command to run when the container starts
#CMD ["mydumper", "-c", "/etc/mydumper.cnf"]
#CMD ["mydumper", "--version"]
#CMD ["mydumper"]
CMD ["echo", "Container started without running mydumper"]