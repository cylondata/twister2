#! /bin/bash

####################################################
# Run many jobs
####################################################

jobs=64
workersPerJob=16

rm pids.txt 2>/dev/null

for ((i=0; i<jobs ;i++)); do
  jobName="j${i}"

  # start job in background
  conf/kubernetes/run-job.sh $jobName $workersPerJob $jobs &
  echo "$!" >> pids.txt
done
