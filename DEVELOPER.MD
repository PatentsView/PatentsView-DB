# Developer Instructions
# Requirements
* Docker
* docker-compose  

#### Change History
1. Updated SQL sample file location
2. Moved Port Number details to separate sectiomn
3. Added Password Section
# Setup Instructions  
#### Note: See Port Numbers section on how to connect to different tools #### 
## Base & Airflow Setup

1. Using recursive to ensure that submodules are installed, clone the repository (cloned local folder referred to as **PROJECT_HOME** for the rest of this documentation)
`git clone https://github.com/PatentsView/PatentsView-DB.git --recursive`
3. Build docker image from **PROJECT_HOME** using `docker build --build-arg UID=$(id -u) --build-arg GID=$(id -g) -t {image-name} .`
4. Using `docker-compose-sample.yaml` as reference, create a file called `docker-compose.yaml`  
`cp -v docker-compose-sample.yaml docker-compose.yaml`
	a. Replace `/Path/to/Project/Root/PatentsView-DB/` with the **PROJECT_HOME** location in all `volumes` lines  (total of 3)  
	b. Replace **{docker-image-name}** with docker image name you used/intend to use. This is the value that you use in `docker build -t . {docker-image-name}`  
	c. Replace **{service-name}** with a suitable name. In the absence of a container name in the `yaml` file, this name will be used as a basis for any docker container that is spawned based on the above image  
	d. Across the yaml file, replace **{airflow\_container\_name}** with a suitable container name. This is the name you will be using in `docker exec` command 
 	e. Across the yaml file, replace **{db\_container\_name}** with a suitable name. This will be the `hostname` at which airflow will connect to MySQL (it doesn't have to be in URL format)    

Example:  
 **image-name**: patent-data-processing  
 **service-name**: patent-data-processor  

4. Run this command from your host system. `docker network create patentsview-pipeline-network` . The name you use here must be used in `docker-compose.yaml` file.

## Credentials
5. Use the following command to create a copy of config_template.ini, named config.ini. Config_template is located in **PROJECT_HOME**.

`cp -v config_template.ini config.ini`

=======

## MySQL Setup  
6. Create a copy of `init-sample.sql` with the name `airflow-metadata-db-sql/init.sql`  
	a. Generate a password for `airflow` mysql user and put that in the file you created above
	b. Use the same password generated in #5.a. in the `environment` section in your docker-compose file. i.e. `AIRFLOW__CORE__SQL_ALCHEMY_CONN: "mysql://airflow:airflow_db_password@airflow-metadata-db:3306/airflow"` 
   	c. Across yaml file, Change "airflow-db-{suffix}" to a suitable image name for MySQL container
7. Generate a mysql root password and use that in the line `MYSQL_ROOT_PASSWORD: mysql_root_password` in `docker-compose.yaml` file
8. Run the following command in your host system: `id -u` and then `id -g` 
9. In the USER line in `MySQL` section of docker-compose.yaml, replace `1000 - 1000` with output from above command  
#### Note: Any SQL file within the `airflow-metadata-db-sql` directory will be executed when MySQL is first setup. ####
## Jupyter Setup  
10. In your `docker-compose.yaml` file, update the line `- 8889:8888` to `yourDesiredPort:8888` and then you can access the notebook at `localhost:yourDesiredPort`.  
11. When you start the container, it outputs the token that you can use in browser when prompted for password  
## Debugging Setup
12. Using  `docker-compose-debug-sample.yaml` as reference, create a file called `docker-compose-debug.yaml`. Make the same changes that you made to `docker-compose.yaml`

## Port Numbers Setup
There are 3 different ports used by different tools. See table below for reference:

| Service/Tool | Within Docker Port Number |  Host System port Number (as specified in sample file)  |
|------------------|---------------------------|---------------------------------------------------------|
| Airflow UI| 8080 | 9090 (Change this in compose file) |
| Jupyter Notebook | 8888 | 8889 (Change this in compose file) |
| MySQL | 3306 | 3308 (Change this in compose file) |

Naturally in a shared system, we need to map this to different port numbers so there are no overlaps. 
* The format that `docker-compose.yaml` uses for port number is : `{host_port_number}:{docker_port_number}`, 
    Eg for airflow UI port: `- 9090:8080` would map the 8080 port from within the docker container to 9090 port in host system. 
* If you are using a shared system, the host system is still not your laptop, it is a server instead. If you want to use these services from your laptop (i.e your browser or MySQL Workbench) you also need to configure your `SSH` tool grant access to the different ports we mapped in above step (& table)  

*  It is unrealistic to add all port numbers to the AWS security rules. The ideal and simplest way to resolve this is using port forwarding using SSH. See Local Port Forwarding [here](https://help.ubuntu.com/community/SSH/OpenSSH/PortForwarding)
	*  Eg: For MySQL `ssh -L 3306:localhost:3308 gp-linux-server`  Read this command as **After logging into the SSH server, make port number 3308 of the server that is accessible at the name "localhost" available at 3306 port in my local machine** (when you are on a server, `localhost` refers to the machine you are logged in to, when you are on your laptop, `localhost` refers to your laptop)   
In effect, if there is a MySQL server running at 3308 in `gp-linux-server`, after running that command you can access that server at `localhost:3306`  
	    * `-L`  : Indicates Local Port forwarding.  
	    *`3306`: The port number that you will to use in your local machine along with localhost (typically your laptop where you run `ssh` command)  
	    * `:` : separator  
	    * `localhost`: This is not your local machine . This is the server you are connecting to, you should not need to change this.  
	    * `3308`: Destination port (this typically comes from docker-compose file). Indicates the port where the data is forwarded when you send data to `3306` from above  
	    * `...`: Rest of your SSH command
	
	* If your local machine is a Windows Machine, and you use tools like **Putty** to connect to SSH server, the same can be achieved using Settings UI. Eg for Putty: [Guide](https://www.linode.com/docs/networking/ssh/ssh-connections-using-putty-on-windows/#port-forwarding-ssh-tunnels-with-putty)
* Update port number in the `docker-compose.yaml` based on your chosen port numbers

## Running the created image
13. Use the command `docker-compose up` to run the airflow & database server. This will print logs to the console. Use `-d` flag to avoid this: `docker-compose up -d`  
	a. 	If you choose to use daemon `-d` flag, you can still see logs using `docker logs -f {container_id/container_name}` command. 
14. Open the UI at `localhost:{desiredPortNumber}` (port number from #3.d or whatever port number you used in SSH tunneling, see section : **Cleanup & Shared Systems** ) 
15. All the airflow related files such as logs, cfg, airflow.db will be available at **PROJECT_HOME/airflow**

16. If the UI does not open or if you want to get into the docker container for debugging purposes use the following command.   
	a.	`docker-compose -f docker-compose-debug.yaml up -d`  
	b. This will start the container but not the scheduler or the webserver. You can use `docker exec` to get into the container and start these programs yourself. 
	
	
## Testing Changes i.e. Re-running containers
When you have made a change to codebase (airflow or otherwise) the changes will be automatically reflected in the running container due to the mounted volume. (For DAG changes, you might have to run `airflow initdb`)  
If your container stops for some reason (either the airflow or database container), you can either start the stopped container manually or simply run `docker-compose up`

## Cleanup & Shared Systems
* If you already completed all the steps in previous version of this instruction, make sure to clean up all docker networks, containers and images when building new version of docker images    
* If you are using a shared system (i.e not a laptop), you might want to suffix your container and network names with your username

## Passwords
1. Jupyter notebook password is prited to console (or to `docker logs` when -d switch is used)
2. Use the password in `init.sql` file along with port number as configured to connect to the MySQL airflow database
3. If you want MySQL `root`  privilege, use the same connection parameters as above but with password  `MYSQL_ROOT_PASSWORD` from `docker-compose.yaml`  

#### Jupyter Notebook Packages   
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
 

# Reporting Database Configuration
Edit the config.ini file with following changes
1. Add Slack API token
2. Update the slack channel in which we want to recieve notifications
3. Update the source `raw` database name
4. Update destination `reporting` database name
