#! /bin/bash

####################################################
# Remove a file from s3 bucket
####################################################

# check supplied parameters
if [[ "$#" -ne 1 ]]; then
  echo "$# arguments supplied. One parameter required: s3File"
  exit 10
fi

s3File=$2

echo "s3File: $s3File"

# upload the job package to s3 bucket
aws s3 rm $s3File

# check whether job package deleted successfully
if [ $? -ne 0 ]; then
  # Since the job package can not be deledet successfully, exit with code 13
  echo "the job package: $s3File can not be deleted successfully."
  exit 13
fi
