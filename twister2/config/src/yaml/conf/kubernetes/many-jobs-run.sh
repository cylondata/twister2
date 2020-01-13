#! /bin/bash

####################################################
# Rrun many jobs
####################################################

jobs=3
workersPerJob=4

for ((i=0; i<jobs ;i++)); do
  jobName="j${i}"

  # start job in background
  conf/kubernetes/run-job.sh $jobName $workersPerJob $jobs &
done
