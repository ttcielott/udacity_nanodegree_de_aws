#!/bin/bash
set -eu

# S3 Gateway Endpoint
# identify the VPC (Where Glue is located) 
# that needs access to S3 (outside of the VPC)

# Since the S3 Service runs in different network, 
# we need to create what's called an S3 Gateway Endpoint.
# This allows S3 traffic from your Glue Jobs into your S3 buckets. 
# Once we have created the endpoint, your Glue Jobs will have a network path to reach S3.
VpcId=$(aws ec2 describe-vpcs | grep "VpcId" | cut -d ':' -f2 | cut -d '"' -f2)

# Routing Table
# Next, identify the routing table you want to configure with your VPC Gateway. 
# You will most likely only have a single routing table if you are using the default workspace. Look for the RouteTableId
RouteTableId=$(aws ec2 describe-route-tables | grep "RouteTableId" | cut -d ':' -f2 | cut -d '"' -f2)

# create the S3 Gateway
aws ec2 create-vpc-endpoint --vpc-id $VpcId --service-name com.amazonaws.us-east-1.s3 --route-table-ids $RouteTableId