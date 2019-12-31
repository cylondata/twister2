#! /bin/bash

# check supplied parameters
if [[ "$#" -ne 3 ]]; then
  echo "$# arguments supplied. Three parameters required: localFile s3File expirationSec"
  exit 10
fi

# presigned download URL will be saved to this file
urlFile=~/.twister2/downloadURL.txt

localFile=$1
s3File=$2
expSec=$3

echo "localFile: $localFile"
echo "s3File: $s3File"
echo "expSec: $expSec"

# upload the job package to s3 bucket
aws s3 cp $localFile $s3File

# check whether job package uploaded successfully
if [ $? -ne 0 ]; then
  # Since the job package can not be uploaded successfully, exit with code 1
  echo "the job package: $localFile can not be uploaded successfully to: $s3File"
  exit 11
fi

# get presigned url and write it to urlFile
echo $(aws s3 presign $s3File --expires-in $expSec) > $urlFile
if [ $? -ne 0 ]; then
  # Since the job package can not be presigned, exit with code 2
  echo "the job package can not be presigned"
  exit 12
fi
