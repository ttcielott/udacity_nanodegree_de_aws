# Installation of Apache Airflow

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

