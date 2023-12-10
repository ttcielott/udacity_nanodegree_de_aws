#!/bin/bash

set -eu

# allow your Glue job read/write/delete access to the bucket and everything in it. 
# You may notice that after s3 there are three colons ::: without anything between them. 
# That is because S3 buckets can be cross-region, and cross AWS account. 
# For example you may wish to share data with a client, or vice versa. Setting up an S3 bucket with cross AWS account access may be necessary.

aws iam put-role-policy --role-name my-glue-service-role --policy-name S3Access --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListObjectsInBucket",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::dana-data-lakehouse"
            ]
        },
        {
            "Sid": "AllObjectActions",
            "Effect": "Allow",
            "Action": "s3:*Object",
            "Resource": [
                "arn:aws:s3:::dana-data-lakehouse/*"
            ]
        }
    ]
}'