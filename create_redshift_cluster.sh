#!/bin/bash

# Set your desired values here
CLUSTER_IDENTIFIER="my-redshift-cluster"
NODE_TYPE="dc2.large"
MASTER_USERNAME="admin"
MASTER_PASSWORD="${DEFAULT_PASSWORD}"
SECURITY_GROUP_ID="sg-12345678"
SUBNET_GROUP_NAME="my-subnet-group"
AVAILABILITY_ZONE="us-east-1a"
MAINTENANCE_WINDOW="Mon:03:00-Mon:03:30"
PARAMETER_GROUP_NAME="default.redshift-1.0"
CLUSTER_TYPE="single-node"
NUMBER_OF_NODES=2
IAM_ROLE_ARN="arn:aws:iam::123456789012:role/my-redshift-role"

# Create the Redshift cluster
aws redshift create-cluster \
  --cluster-identifier "$CLUSTER_IDENTIFIER" \
  --node-type "$NODE_TYPE" \
  --master-username "$MASTER_USERNAME" \
  --master-user-password "$MASTER_PASSWORD" \
  --vpc-security-group-ids "$SECURITY_GROUP_ID" \
  --cluster-subnet-group-name "$SUBNET_GROUP_NAME" \
  --availability-zone "$AVAILABILITY_ZONE" \
  --preferred-maintenance-window "$MAINTENANCE_WINDOW" \
  --cluster-parameter-group-name "$PARAMETER_GROUP_NAME" \
  --cluster-type "$CLUSTER_TYPE" \
  --number-of-nodes "$NUMBER_OF_NODES" \
  --publicly-accessible "$PUBLIC_ACCESS_FLAG" \
  --iam-roles "$IAM_ROLE_ARN" \

