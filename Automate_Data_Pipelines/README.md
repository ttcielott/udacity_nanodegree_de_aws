# Automate Data Pipeline

## Installation of Apache Airflow

Visit [this page](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html) and read the instruction.

I used the following command line as my python enviroment is 3.10 an the latest airflow version is 2.8.1.

```bash
pip install "apache-airflow[celery]==2.8.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"
```

The following is the URL of a contraint file.

```text
https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt
```

Replace `${AIRFLOW_VERSION}` and `${PYTHON_VERSION}` according to your requirement and check if the URL opens.

## Installation of MySQL Database

1. Open Docker

2. Run the following command on terminal

    ```bash
    cd mysql_setup && docker compose up -d
    ```

## MySQL DB Setup for Airflow

You need to create a database and a database user that Airflow will use to access this database.

1. Open the following URL.

    [http://localhost:8080/](http://localhost:8080/)

2. Login with the following detail

    System: MySQL
    Server: db
    Username: root
    Password: my-root-secret

3. Set up a MySQL Database for Airflow

    Click 'SQL Command' on the right and the run the following SQL statement.

    ```sql
    CREATE DATABASE airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    CREATE USER 'airflow_user' IDENTIFIED BY 'airflow_pass';
    GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow_user';
    ```

4. Install mysqlconnector

    ```bash
    pip install mysql-connector-python
    ```

5. Reinitialise the Airflow database in MySQL with the following command

      ```bash
      airflow db init
      ```

## Create an Airflow User

This user will be used to login into the Airflow UI and perform some admin functions

```bash
 airflow users create \
          --username admin \
          --firstname FIRST_NAME \
          --lastname LAST_NAME \
          --role Admin \
          --email admin@example.org
```

Set your passowrd when promt asks.  

## Run the Webserver

Make sure mysql docker container is running.

1. Run the scheduler.

    ```bash
    airflow scheduler
    ```

2. Launch another terminal, activate the virtual environment if it isn't.
    If we are using the port 8080 for adminer, change the port for airflow by typing.

    ```bash
    airflow webserver --port 8090
    ```

3. Open the following URL

    [http://localhost:8090/](http://localhost:8090/)

4. Log into the user interface using the user name created earlier with "airflow users create".

    - You can terminate `airflow scheduler` & `airflow webserver` by simply hitting `Ctrl + C` on command line prompt.
