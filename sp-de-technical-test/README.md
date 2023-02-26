### How to set up the environment?

- Before starting, install Compose. Instructions here => https://docs.docker.com/compose/install/
- Change to the root of the project directory
- Run ```docker-compose up```. The container should be now up and running.
- Open a new terminal and run ```docker exec -it sp_postgres bash``` to access the container console
- Once inside run the database initialization script: ```python3 src/scripts/init.py```
- At this point you should have a schema called sp_technical_test with a dummy table called sp_test

### Database credentials:

- host: sp_db (or localhost)
- port: 5432
- user: postgres
- password: sp_technical_test
- database: postgres
- schema: sp_technical_test

Please make sure you create all needed database objects in the sp_technical_test schema. This is the schema we will look
at when reviewing the test.

### How to install more python libraries?

We managed to solve all the tasks with the current libraries, but in case you would like to add some more follow these steps: 

- Add the library in requeriments.txt. It has to be a library available on https://pypi.org 
- Stop docker-compose, in case it is running, with either Ctrl+D from the terminal where it's running or by running ```docker compose stop``` with a new console from the root of the project directory
- From the root of the project directory run ```docker-compose build```
- Start the container with ```docker-compose up```
- Open a new terminal and run ```docker exec -it sp_postgres bash``` to access the container console

### How to run the scripts?

Task #1

- Type your code in src/scripts/load_raw_data.py and run ```python3 src/scripts/load_raw_data.py``` from the container console

Task #2

- Type your code in src/scripts/load_currency_exchange.py and run ```python3 src/scripts/load_currency_exchange.py``` from the container console

Task #3

- Type your SQL in src/sql_projects/etl/queries/etl.sql and run ```python3 src/scripts/etl.py``` from the container console

Task #4

- No script  

Task #5

- No script

If you have any doubt, please do not hesitate to contact us at data.engineering@socialpoint.es and we will be happy to help you.