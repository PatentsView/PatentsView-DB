# Developer Instructions
# Requirements
* Docker
* docker-compose
# Airflow/Docker setup Instructions

1. Clone into this repository (cloned local folder referred to as **PROJECT_HOME** for the rest of this documentation)
2. Build docker image from **PROJECT_HOME** using `docker build -t {image-name} .`
3. Using `docker-compose-sample.yaml` as reference, create a file called `docker-compose.yaml`  
  a. Replace `/Path/to/Project/Root/PatentsView-DB/` should be replced with the **PROJECT_HOME** location in both `volumes` lines  
  b. Replace **{docker-image-name}** with docker image name you used/intend to use. This is the value that you use in `docker build -t . {docker-image-name}`  
  c. Replace **{service-name}** with any string. This name will be used as a basis for any docker container that is spwaned based on the above image  
Example:  
 **image-name**: patent-data-processing  
 **service-name**: patent-data-processor  
 d. Update port number in this line: `- 9090:8080` if you want to use a different port number than 8080 to access airflow UI. **Format: {desiredPortNumber}:8080**
4. Using  `docker-compose-debug-sample.yaml` as reference, create a file called `docker-compose-debug.yaml`. Make the same changes as #3
5. Use the command `docker-compose up` to run the airflow server. This will print logs to the console. Use `-d` flag to avoid this: `docker-compose up -d`
6. Open the UI at `localhost:{desiredPortNumber}` (port number from #3.d)
7. All the airflow related files such as logs, cfg, airflow.db will be available at **PROJECT_HOME/airflow**
8. If the UI does not open or if you want to get into the docker container for debugging purposes use the following command.   
	a.	`docker-compose -f docker-compose-debug.yaml up -d`  
	b. This will start the container but not the scheduler or the webserver. You can use `docker exec` to get into the container and start these programs yourself. 
