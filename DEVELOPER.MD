# Developer Instructions

# Requirements

* Docker
* docker-compose v2 (make sure its v2)

# Pre-requisites

* Logged into Dockerhub
* `patentsview/airflow` image is available on dockerhub

# Docker Image

## When to (re-)build the docker image

Whenever the underlying airflow image has changed. Refer to
the [Pipline Image repo](https://github.com/PatentsView/PatentsView-Pipeline-Setup) to learn about when and how to build
that image

## How to build

* Ensure you are logged into dockerhub.
* Run the command:

```shell
docker buildx build \
 --output type=registry \
 --platform linux/amd64,linux/arm64 . \
--build-arg PYTHON_BASE_IMAGE="0.1" \
 --tag patentsview/update_pipeline_{YYYYMMDD}`
```

# Docker Container Organization

There are at least 7 containers that will be running:

| Container  | Underlying Image | Server                 | Compose File Name                          | Service Name  | Container Port Number/Host Port Number | Command                                 | 
|------------|------------------|------------------------|--------------------------------------------|---------------|----------------------------------------|-----------------------------------------|
| Airflow Metadata database  | mariadb:latest | GP Server              | docker-compose-db.yml                      |   pv_updater_meta_db  | 3306/3308                              | Default Container command               |
| Airflow DB init/upgrade runner  | patentsview:airflow | GP Server              | docker-compose.yaml                        |   pv_data_pipeline_initializer  | N/A                                    | airflow db init                         |
| RabbitMQ Message Broker |  rabbitmq:3-management | GP Server              | docker-compose.yaml                        |   rabbitmq  | - 15672/8188 <br> - 5672/5672          | Default Container command               |
| Airflow Scheduler  | patentsview:airflow | GP Server              | docker-compose.yaml                        |   pv_data_pipeline_scheduler  | N/A                                    | airflow schduler                        |
| Airflow Webserver  | patentsview:airflow | GP Server              | docker-compose.yaml                        |   pv_data_pipeline_webserver  | 8080/9090                              | airflow webserver                       |
| Airflow Flower  | patentsview:airflow | GP Server              | docker-compose.yaml                        |   pv_data_pipeline_celery_flower  | 5555/5555                              | airflow celery flower                   |
| Airflow Admin Worker  | patentsview:airflow | GP Server              | docker-compose-admin-worker.yaml           |   pv_data_pipeline_admin_worker  | 8793/8793                              | airflow celery worker -q admin          |
| Airflow Data Collection Worker  | patentsview:airflow | Data Collection Server | docker-compose-data-collection-worker.yaml |    pv_data_pipeline_data_collection_worker |     8793/8793                                   | airflow celery worker -q data_collector |
| Airflow Disambiguation Worker  | patentsview:airflow | Disambiguation Server  | docker-compose-disambiguator.yaml                                          |    pv_data_pipeline_disambiguation_worker |                          8793/8793              | airflow celery worker -q disambiguation |

### airflow_pipeline_env.sh

- Sensitive variables should be stored in `airflow_pipeline_env.sh`. See `airflow_pipeline_env.sh.template` for
  reference
- The variables need to be same in each server where any pipeline container is deployed

## init-sample.sql

- Make a copy of this file as airflow-metadata-db-sql/init.sql
- Replace placeholder password with a generated password. You'll use this in the next section

## docker-compose-db.yaml

- Contains the configuration required to start the database. This includes
    - An init sql that creates the database and username required for airflow (Use password from previous section)
    - The root password configuration
    - Mounted volume configuration where the database files are stored
- See `docker-compose-db.yml.template` for reference

## docker volume (Developer machine)

- On developer machines (non-production), you'll need to create the data volume like so:
  `docker volume create pv_update_volume`

## docker network create

- The first time you set this up on each machine, you'll need to create a network using the command
  `docker network create patentsview-pipeline-network`

## docker-compose.yaml

- Contains all, but one, service that needs to run on the GP Server. These are
    - RabbitMQ
    - Flower
    - Scheduler
    - Webserver
    - Initializer

### Initializer

- Initializer is solely meant for running airflow for the first time
    - The same container can also be used to upgrade the airflow database with the
      command: ` docker-compose  run --rm  pv_data_pipeline_initializer db upgrade  `
- Initializer container is expected to run and exit.

### Flower, Initializer, Scheduler & Webserver

- These are all based on `patentsview/airflow` container
- The docker compose configuration differ only in two ways:
    - The command is different (airflow scheduler v airflow webserver for example)
    - Port numbers exposed are different

### Worker docker-compose files

- docker-compose-admin-worker.yml, docker-compose-disambiguator.yaml, docker-compose-worker.yaml all have the same
  configuration as other `patentsview/airflow` image, again, except for the command and port numbers
