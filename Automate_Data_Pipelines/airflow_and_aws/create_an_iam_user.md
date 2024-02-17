# Create an IAM User in AWS

1. Go to `IAM` in AWS console and click `users`.
2. And then select, `Create user`.
3. Add username and click next.
4. Select `Attach policies directly`.

    Search and select the following policies:
    - AdministratorAccess
    - AmazonRedshiftFullAccess
    - AmazonS3FullAccess
5. Click `Create user`.
6. Create a credential to access with this user name, so click on the username first.
7. Select `Security credentials` tab.
8. Click `Create access key`.
9. Select `Other` as the user case and the click `Next`.
10. Click `Create access key`.
11. Click on `Download .csv file` to download the credentials you just generated. You will need these credentials to configure your AWS connection in Airflow later.

# Add Airflow Connections

Here, we'll use Airflow's UI to configure your AWS credentials.

1. To go to the Airflow UI:
2. Click on the `Admin` tab and select `Connections`.
3. Under Connections, click the plus button or you can edit `aws_default`.
4. On the create connection page, enter the following values:
    - **Connection Id**: Enter `aws_credentials`.
    - **Connection Type**: Enter `Amazon Web Services`.
        - my local airflow didn't have `Amazon Web Services` type, so I installed the provider from the [atronomer webiste's latest AWS provider](https://registry.astronomer.io/providers/apache-airflow-providers-amazon/versions/latest).

    

        Before installation, switch off airflow scheduler and webserver. (`Ctrl + C` if you still have terminal running them, or find all pids using the port number by running `lsof -wni -tcp:[port]` and then `kill -9 [pid]`)


        If you click `User Prodvider` on the top right, you can get the command line like the following one.:

        ```bash
        pip install apache-airflow-providers-amazon==8.17.0
        ```

        Once the provider is installed, you start `airflow schedueler` and `webserver` again.

        ```bash
        airflow scheduler
        ```

        ```bash
        airflow webserver -p 8090
        ```

    - **AWS Access Key ID**: Enter your `Access key ID` from the IAM - User credentials you downloaded earlier.
    - **AWS Secret Access Key**: Enter your Secret access key from the IAM User credentials you downloaded earlier. Once you've entered these values, select `Save`.
    - Click the `Test` button to pre-test your connection information.

# Using the S3Hook with your AWS Connection

Now you can reference your AWS Credentials without storing them in the code, for example:

```bash
from airflow.hooks.S3_hook import S3Hook
. . .
hook = S3Hook(aws_conn_id='aws_credentials')
        bucket = Variable.get('s3_bucket')
        prefix = Variable.get('s3_prefix')
        logging.info(f"Listing Keys from {bucket}/{prefix}")
        keys = hook.list_keys(bucket, prefix=prefix)
        for key in keys:
            logging.info(f"- s3://{bucket}/{key}")
    list_keys()

list_keys_dag = list_keys()

```