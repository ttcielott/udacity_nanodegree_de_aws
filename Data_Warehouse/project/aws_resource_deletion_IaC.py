## Clean up your resources
### Infrastructure-as-code

import pandas as pd
import boto3
import time

# # Load DWH Params from a file
import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")



# Create clients for IAM, and Redshift
import boto3

iam = boto3.client('iam',                   
                   region_name = 'us-west-2',
                   aws_access_key_id = KEY,
                   aws_secret_access_key = SECRET)


redshift = boto3.client('redshift',                   
                   region_name = 'us-west-2',
                   aws_access_key_id = KEY,
                   aws_secret_access_key = SECRET)


# delete redshift cluster
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)


# delete the created resources
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)








