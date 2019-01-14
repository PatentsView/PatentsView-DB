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
# Jupyter Notebook
This docker container comes with a jupyter notebook with a list of commonly used python packages pre-installed, including,   
1. beautifulsoup4==4.6.3  
2. celery==4.2.1  
3. configparser==3.5.0  
4. dask==1.0.0  
5. Flask==0.12.4  
6. future==0.16.0  
7. google-api-core==1.7.0  
8. gunicorn==19.9.0  
9. Jinja2==2.8.1  
10. lxml==4.3.0  
11. Markdown==2.6.11  
12. matplotlib==2.2.3  
13. mysqlclient==1.3.14  
14. nltk==3.4  
15. numpy==1.13.3  
16. pandas==0.23.4  
17. psycopg2-binary==2.7.6.1  
18. PyHive==0.6.1
19. pymongo==3.7.2
20. PyMySQL==0.9.3
21. redis==3.0.1  
22. requests==2.21.0  
23. scikit-learn==0.20.1  
24. scipy==1.1.0  
25. seaborn==0.9.0  
26. SQLAlchemy==1.2.15  
27. statsmodels==0.9.0
(Run `pip freeze` from within container to see full list).
  
The notebook also comes with a list of useful extension:   
![Extensions](extensions.png) Any extensions you enable would be temporary. To permanently enable an extension you have to update the Dockerfile 
## Access notebook
1. In your `docker-compose.yaml` file, update the line `- 8889:8888` to `yourDesiredPort`:8888` and then you can access the notebook at `localhost:yourDesiredPort`.
2. When you start the container, it outputs the token that you can use in browser when prompted for password   

