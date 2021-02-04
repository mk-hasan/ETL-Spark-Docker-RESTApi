## Project Structure:

-etl
    -src
	-configs
		-etl_config.json
	-job
		-etl_job.py
    -resources
	-data.json
	-postgresdriver.jar

    -docker-compose.yml


etl_config.json : The confguration details for the database and input data

etl_job.py : The etl job module which will trigger the ETL pipeline

data.json : This is the input json data

postgresdriver.jar: The driver file for postgres JDBC connection.

docker-compose.yml : The docker compose file that will start the spark container and the postgres database.

***********************************************START THE APP**********************************************
**********************************************************************************************************

a) Necessary changes in docker-compose.yml file:
	1. Open the file and change the follwing details in pyspark-notebook container:
		-     volumes:
        		- '$YOUR_DIRECTORY_PATH/data-engineer-challenge-python/data-engineer-challenge-python/etl:/home/jovyan/work'

	*** This will link you local working directory into the spark container working directory. Any changes in the local directory now will also be changed in the container directory.

	2. Up the docker containers using follwoing command from the /etl folder:
		- docker-compose up

b) Now all the container will be running. The containers are:
	1. pyspark-notebook
	2. postgres
	3. pgAdminer(to access the database UI)

**********************************************RUN THE ETL JOB********************************************
*********************************************************************************************************

a) After running the containes we can see all the containers are running and now we can submit our etl spark job using the follwing command:

		-> docker exec -i -t "container_id*" /usr/local/spark/bin/spark-submit --driver-class-path "jdbc postgres driver path" --jars "jdbc postgres driver path" "etl job py file "
		
		-> Example: docker exec -i -t 70e3fa090d7d /usr/local/spark/bin/spark-submit --driver-class-path /home/jovyan/work/resources/postgresql-42.2.5.jre6.jar --jars /home/jovyan/work/resources/postgresql-42.2.5.jre6.jar /home/jovyan/work/src/job/etl_job.py

* Container id can be found using "docker ps -all"

Actually, you should run the example command from the above line just changing the container id , it will start the job and finish it. 
Then you can check the created data that saved int the postgres sql table by visiting Adminer container:
		
		-> http://localhost:8080/

That is it. Now we can use the API service to get the data. Remember the continer should be running as the postgres should be running.

N.B: if you want to change the database details or input data then please chage into the etl_config.json file. It will be automatically loaded when the etl job starts

Thank you.



