#! /bin/bash

# check supplied parameters
if [[ "$#" -ne 2 ]]; then
  echo "$# arguments supplied. Two parameters required: localFile s3File"
  exit 10
fi

localFile=$1
s3File=$2

echo "localFile: $localFile"
echo "s3File: $s3File"

# upload the job package to s3 bucket
aws s3 cp $localFile $s3File

# check whether job package uploaded successfully
if [ $? -ne 0 ]; then
  # Since the job package can not be uploaded successfully, exit with code 1
  echo "the job package: $localFile can not be uploaded successfully to: $s3File"
  exit 11
fi
