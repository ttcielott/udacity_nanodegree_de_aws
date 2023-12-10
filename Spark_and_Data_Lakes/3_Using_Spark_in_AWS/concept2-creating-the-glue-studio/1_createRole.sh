#!/bin/bash

set -eu

# create a role that the Glue Service Role can assume to act on your behalf
# for AWS Glue to act on your behalf to access S3 and othe resources, 
# you need to grant access to the Glue Service by creating an IAM Service Role that can be assumed by Glue
aws iam create-role --role-name my-glue-service-role --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}'