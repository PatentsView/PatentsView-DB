# Developer Notes To Run PatentsView Data Pipeline

## Set Up Instructions:

### Requirements
* Docker
* docker-compose v2 (make sure its v2)
* Logged into Dockerhub
* `patentsview/airflow:{tag}` image is available on dockerhub
* Sensitive variables should be stored in `airflow_pipeline_env.sh`. See `airflow_pipeline_env.sh.template` for
  reference
  - The variables need to be same in each server where any pipeline container is deployed
  
### Docker Image

#### How to build
* Ensure you are logged into dockerhub.
* Run the command:

#### When to (re-)build the docker image
Whenever the underlying airflow image has changed. Refer to
the [Pipline Image repo](https://github.com/PatentsView/PatentsView-Pipeline-Setup) to learn about when and how to build
that image

```shell
docker buildx build \
 --output type=registry \
 --platform linux/amd64,linux/arm64 . \
--build-arg PYTHON_BASE_IMAGE="0.1" \
 --tag patentsview/airflow:{tag}`
```

## Docker Container Organization & Explanation

There are at least 6 containers with unique services running:

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

### init-sample.sql

- Make a copy of this file as airflow-metadata-db-sql/init.sql
- Replace placeholder password with a generated password. You'll use this in the next section

### docker-compose-db.yaml

- Contains the configuration required to start the database. This includes
    - An init sql that creates the database and username required for airflow (Use password from previous section)
    - The root password configuration
    - Mounted volume configuration where the database files are stored
- See `docker-compose-db.yml.template` for reference

### docker volume (Developer machine)

- On developer machines (non-production), you'll need to create the data volume like so:
  `docker volume create pv_update_volume`
- Volumes connect to the host server so that data will be saved. Otherwise, everything in the container is deleted everytime the container is closed.

| Volume      | Purpose                                                 | Servers                | 
|-------------|---------------------------------------------------------|------------------------|
| data-volume | Store XML rawdata                                       | PV-DataCollector; PVGP |
| app-volume  | Store PGPUBS XML rawdata                                    | PV-DataCollector; PVGP |
| archive     | Store archived weekly PV databases (e.g. upload_20100422) | PV-DataCollector; PVGP |

### docker network create

- The first time you set this up on each machine, you'll need to create a network using the command
  `docker network create patentsview-pipeline-network`

### .env.template (Only on Linux machines)

- Make a copy of .env.template to .env and se the `HOST_UID` value to the result of running this command: `id -u`
- Note that the airflow container MUST HAVE THE SAME UID as the server and that the GID MUST BE SET TO 0

## docker-compose.yaml - Explanation of Services

- Contains all, but one, service that needs to run on the GP Server. These are
  - Initializer
  - RabbitMQ
  - Flower
  - Scheduler 
  - Webserver

### Initializer

- Initializer is solely meant for running airflow for the first time
    - The same container can also be used to upgrade the airflow database with the
      command: ` docker-compose  run --rm  pv_data_pipeline_initializer db upgrade`
- Initializer container is expected to run and exit.

### Celery Executor/ RabbitMQ / Flower
- [CeleryExecutor]((https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/celery.html)) (Task Queue software) is one of the ways you can scale out the number of workers. For this to work, you need to setup a Celery backend, with`RabbitMQ` (Message Broker), and change your airflow.cfg to point the executor parameter to CeleryExecutor and provide the related Celery settings.
- [Flower](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/security/flower.html) is a web based tool for monitoring and administrating Celery clusters.
- [RabbitMQ]() is a Message Broker that recieves messages from Airflow scheduler and sends them to the workers (queues). 

## Worker docker-compose files - Configuration Details

- docker-compose-admin-worker.yml, docker-compose-disambiguator.yaml, docker-compose-worker.yaml all have the same
  configuration as other `patentsview/airflow` image, again, except for the command and port numbers
  - The worker `queues` execute the python code in Airflow. Different DAGs run using different workers and are controlled using different pools. For the latest configuration, see the [DAGS](airflow/dags) folder
- Note that all DAG processes listed below also need to have `PVGP server` running to run airflow

| DAG                                | Worker (Queue)                 | Pools                                                          | Servers                             | 
|------------------------------------|--------------------------------|----------------------------------------------------------------|-------------------------------------|
| Granted Patent Weekly Parser       | data_collector                 | high_memory_poool & default_pool                               | PV-DataCollector                    | 
| Pre-grant Weekly Parser            | data_collector                 | database_write_iops_contenders & default_pool                  | PV-DataCollector                    | 
| Classifications Parser             | data_collector                 | database_write_iops_contenders & default_pool                  | PV-DataCollector                    | 
| Lawyer Disambiguation              | data_collector & disambiguator | high_memory_pool & default_pool                                | PV-DataCollector & PV-Disambiguator | 
| Assignee & Inventor Disambiguation | data_collector & disambiguator | high_memory_pool, database_write_iops_contenders, default_pool | PV-DataCollector & PV-Disambiguator | 
| Reporting DB                       | disambiguator | default_pool                                                   | PV-Disambiguator                    | 

An [Airflow Pool](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html) is an isolated space where a task can run. Each pool has a fixed number of slots and each task uses one slot. Each task without an explicit assigned pool is assigned to `default_pool` which has 128 slots.
* For example, if a pool named `small_pool` was allocated 3 slots and 5 tasks were queued in that pool, only 3 could run simultaneously.