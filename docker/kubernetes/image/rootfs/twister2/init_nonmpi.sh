#! /bin/bash
####################################################################
# This script starts a Twister2 worker in jobs that do not use openmpi
# It:
#   gets the job package and unpacks it, if it is the first worker in a pod
#   if it is not the first worker, it waits for the first worker to get the job package and unpacks it
#   sets the classpath
#   starts the worker class
####################################################################

# get the job package first
./get_job_package.sh

# check whether job package downloaded successfully
if [ $? -ne 0 ]; then
  echo "Since the job package can not be retrieved, sleeping to infinity"
  sleep infinity
fi

# update the classpath with the user job jar package
CLASSPATH=$POD_MEMORY_VOLUME/$JOB_ARCHIVE_DIRECTORY/$USER_JOB_JAR_FILE:$CLASSPATH

# start the class to run
echo "Starting $CLASS_TO_RUN .... "
java $CLASS_TO_RUN
echo "$CLASS_TO_RUN is done. Starting to sleep infinity ..."
sleep infinity
