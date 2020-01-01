#! /bin/bash

# check supplied parameters
if [[ "$#" -ne 2 ]]; then
  echo "$# arguments supplied. Two parameters required: s3File expirationSec"
  exit 10
fi

# presigned download URL will be saved to this file
urlFile=~/.twister2/downloadURL.txt

s3File=$1
expSec=$2

echo "s3File: $s3File"
echo "expSec: $expSec"

# get presigned url and write it to urlFile
echo $(aws s3 presign $s3File --expires-in $expSec) > $urlFile
if [ $? -ne 0 ]; then
  # Since the job package can not be presigned, exit with code 2
  echo "the job package can not be presigned"
  exit 12
fi
