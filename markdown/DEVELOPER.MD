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
  - The worker `queues` execute the python code in Airflow. Different DAGs run using different workers and are controlled using different pools. For the latest configuration, see the [DAGS](../airflow/dags) folder
- Note that all DAG processes listed below also need to have `PVGP server` running to run airflow

| DAG                                | Worker (Queue)                 | Pools                                                         | Servers                             | 
|------------------------------------|--------------------------------|---------------------------------------------------------------|-------------------------------------|
| Granted Patent Weekly Parser       | data_collector                 | high_memory_pool & default_pool                               | PV-DataCollector                    | 
| Pre-grant Weekly Parser            | data_collector                 | database_write_iops_contenders & default_pool                 | PV-DataCollector                    | 
| Classifications Parser             | data_collector                 | database_write_iops_contenders & default_pool                 | PV-DataCollector                    | 
| Lawyer Disambiguation              | data_collector & disambiguator | high_memory_pool & default_pool                               | PV-DataCollector & PV-Disambiguator | 
| Assignee & Inventor Disambiguation | data_collector & disambiguator | high_memory_pool, database_write_iops_contenders, default_pool | PV-DataCollector & PV-Disambiguator | 
| Reporting DB                       | disambiguator | default_pool                                                  | PV-Disambiguator                    | 

An [Airflow Pool](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html) is an isolated space where a task can run. Each pool has a fixed number of slots and each task uses one slot. Each task without an explicit assigned pool is assigned to `default_pool` which has 128 slots.
* For example, if a pool named `small_pool` was allocated 3 slots and 5 tasks were queued in that pool, only 3 could run simultaneously.

# DUBUGGING GUIDE 

## ENV
1. Check if containers are running 
2. Check which processes are running in each container using `docker top {id}`. This should be some variation of airflow scheduler  for scheduler container, webserver for webserver container so forth
3. Check the UI to see if there are any python errors
4. Check UI to see if there are any heartbeat error/warnings
5. Then check flower UI to see if "clients" are registered. client = each process that runs some version of airflow celery worker command
   1. `ssh -L 5555:localhost:5555 PVGP`
   2. http://localhost:5555/
6. Then check rabbitMQ UI. Particularly the "consumer" section in the queue that you are interested in. For example, if the task is supposed to be under the "data collection" queue, you can see if a consumer is registered under that queue (consumer ~= server/worker process)
   1. `ssh -L 8188:localhost:8188 PVGP`
   2. http://localhost:8188/
   3. guest:guest

## Updating SSL
1. Allow access to the world (0.0.0.0) for http port on the GP Linux server in AWS
2. `SSH PVGP`
3. `sudo -i`
4. `certbot` 
Output below:

```[root@ip-172-31-8-197 ~]# certbot
Saving debug log to /var/log/letsencrypt/letsencrypt.log
Plugins selected: Authenticator nginx, Installer nginx
Starting new HTTPS connection (1): acme-v02.api.letsencrypt.org
Which names would you like to activate HTTPS for?
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
- - - - - - - -
1: pipeline.patentsview.org
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
- - - - - - - -
Select the appropriate numbers separated by commas and/or 
spaces, or leave input
blank to select all options shown (Enter 'c' to cancel): 1
Cert is due for renewal, auto-renewing...
Renewing an existing certificate for pipeline.patentsview.org
Performing the following challenges:
http-01 challenge for pipeline.patentsview.org
Waiting for verification...
Cleaning up challenges
Deploying Certificate to VirtualHost 
/etc/nginx/conf.d/00_airflow.conf
Traffic on port 80 already redirecting to ssl in 
/etc/nginx/conf.d/00_airflow.conf
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
- - - - - - - -
Your existing certificate has been successfully renewed, and the
new certificate
has been installed.
The new certificate covers the following domains:
https://pipeline.patentsview.org
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
- - - - - - - -
IMPORTANT NOTES:
 - Congratulations! Your certificate and chain have been saved at:
  
/etc/letsencrypt/live/pipeline.patentsview.org-0001/fullchain.pem
   Your key file has been saved at:
/etc/letsencrypt/live/pipeline.patentsview.org-0001/privkey.pem
   Your cert will expire on 2023-12-04. To obtain a new or tweaked
   version of this certificate in the future, simply run certbot again
   with the "certonly" option. To non-interactively renew *all* of
   your certificates, run "certbot renew"
 - If you like Certbot, please consider supporting our work by:
   Donating to ISRG / Let's Encrypt:   
https://letsencrypt.org/donate
   Donating to EFF:  https://eff.org/donate-le
```
5. Remove SG rule from #1 in AWS