# Creating Redshift Cluster using the AWS python SDK 
### Infrastructure-as-code

import pandas as pd
import boto3
import json
import time


# # STEP 0: Make sure you have an AWS secret and access key
# 
# - Create a new IAM user in your AWS account
# - Give it `AdministratorAccess`, From `Attach existing policies directly` Tab
# - Take note of the access key and secret 
# - Edit the file `dwh.cfg` in the same folder as this notebook and fill
# [AWS]
# KEY= YOUR_AWS_KEY
# SECRET= YOUR_AWS_SECRET

# # Load DWH Params from a file
import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

variable_df = pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })


# ## Create clients for EC2, S3, IAM, and Redshift
import boto3

ec2 = boto3.resource('ec2',
                   region_name = 'us-west-2',
                   aws_access_key_id = KEY,
                   aws_secret_access_key = SECRET)

s3 = boto3.resource('s3',                   
                   region_name = 'us-west-2',
                   aws_access_key_id = KEY,
                   aws_secret_access_key = SECRET)

iam = boto3.client('iam',                   
                   region_name = 'us-west-2',
                   aws_access_key_id = KEY,
                   aws_secret_access_key = SECRET)


redshift = boto3.client('redshift',                   
                   region_name = 'us-west-2',
                   aws_access_key_id = KEY,
                   aws_secret_access_key = SECRET)


# ## STEP 1: IAM ROLE
# - Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
# TODO: Create the IAM role
try:
    assume_role_policy_document = json.dumps({"Version": "2012-10-17",
                                    "Statement": [
                                        {"Effect": "Allow",
                                         "Principal": {
                                             "Service": "redshift.amazonaws.com"
                                         },
                                         "Action": "sts:AssumeRole"
                                        }
                                    ]
                                })
    
    print('1.1 Creating a new IAM Role')
    
    iam.create_role(RoleName = DWH_IAM_ROLE_NAME, AssumeRolePolicyDocument = assume_role_policy_document)
except Exception as e:
    print(e)    

# TODO: Attach Policy
print('1.2 Attaching Policy')
def attach_iam_policy(role_name, policy_arn):
    """
    The function attaches an IAM policy to a role in AWS.
    
    Args:
      role_name: The name of the IAM role to which you want to attach the policy. This role will have
    the permissions defined in the policy attached to it.
      policy_arn: The `policy_arn` parameter is the Amazon Resource Name (ARN) of the IAM policy that
    you want to attach to the IAM role.
    """
    response = iam.attach_role_policy(RoleName = role_name,
                               PolicyArn = policy_arn)

attach_iam_policy(DWH_IAM_ROLE_NAME,'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess') 

# TODO: Get and print the IAM role ARN
print('1.3 Get the IAM role ARN')
roleArn = iam.get_role(RoleName = DWH_IAM_ROLE_NAME)['Role']['Arn']



# ## STEP 2:  Redshift Cluster
# - Create a RedShift Cluster
# - For complete arguments to `create_cluster`, see [docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.create_cluster)
# *Describe* the cluster to see its status
# - wait until the cluster status becomes `Available`

def prettyRedshiftProps(props):
    """
    The function `prettyRedshiftProps` takes a dictionary of Redshift properties and returns a pandas
    DataFrame with selected key-value pairs.
    
    Args:
      props: A dictionary containing the properties of a Redshift cluster.
    
    Returns:
      a pandas DataFrame object that contains the specified properties from the input dictionary. The
    DataFrame has two columns: "Key" and "Value".
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


try:
    response = redshift.create_cluster(        
        # TODO: add parameters for hardware
        # TODO: add parameters for identifiers & credentials
        # TODO: add parameter for role (to allow s3 access)
        DBName = DWH_DB,
        ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
        ClusterType = DWH_CLUSTER_TYPE,
        NodeType = DWH_NODE_TYPE,
        MasterUsername = DWH_DB_USER,
        MasterUserPassword = DWH_DB_PASSWORD,
        Port = int(DWH_PORT),
        AllowVersionUpgrade = True,
        NumberOfNodes = int(DWH_NUM_NODES),
        PubliclyAccessible=True,
        Encrypted=True,
        IamRoles=[roleArn]
    )

    # give Redshift 10 seconds to finish creating the cluster
    time.sleep(10)


    # set timeout to 900 seconds (15 minutes) to avoide infinite loop
    timeout = time.time() + 900

    # until a cluster status is availble, keep waiting another 10 seconds
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    while myClusterProps['ClusterStatus'] != 'available' and time.time() < timeout:
        time.sleep(10)

        # renew the variable, myClusterProps
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    if myClusterProps['ClusterStatus'] == 'available':
        print("Cluster is ready!")
    else:
        print("Timeout reached. Cluster is not available.")

except Exception as e:
    print(e)


# 2.1 Take note of the cluster endpoint and role ARN
# DO NOT RUN THIS unless the cluster status becomes "Available" 
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print('cluster endpoint: ', DWH_ENDPOINT)
print('cluster role arn: ', DWH_ROLE_ARN)

# ## STEP 3: Open an incoming  TCP port to access the cluster ednpoint
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    # make redshift security group as default security group
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    
    defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,  # TODO: fill out
        CidrIp='0.0.0.0/0',  # TODO: fill out
        IpProtocol='tcp',  # TODO: fill out
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )

    print('Incoming TCP port is opened. Now you can access the cluster.')

except Exception as e:
    print(e)


