#! /bin/bash
####################################################################
# This script starts a Twister2 worker in jobs that use openmpi
# It:
#   It starts an sshd server for password free ssh access
#   gets the job package and unpacks it, all pods are supposed to start only one container
#   mpirun will start the worker processes in pods
#
# if this is the first pod in the job
#   sets the classpath
#   starts the mpimaster class
# if this is not the first pod, this script will just wait to infinity
# mpirun will start the worker process later on
####################################################################

###################  first start sshd #############################
/start_sshd.sh >/tmp/sshd.log 2>&1 &

# if it can not start sshd, exit with a log message
return_code=$?
if [ $return_code -ne 0 ]; then
  echo -n "$return_code" > /dev/termination-log
  exit $return_code
fi

# get the sshd process id
echo "sshd started pid=$(ps auwx |grep [s]sh |  awk '{print $2}')"

###################  get the job package #############################
./get_job_package.sh

# check whether job package downloaded successfully
if [ $? -ne 0 ]; then
  echo "Since the job package can not be retrieved, sleeping to infinity"
  sleep infinity
fi

# update the classpath with the user job jar package
CLASSPATH=$POD_MEMORY_VOLUME/$JOB_ARCHIVE_DIRECTORY/$USER_JOB_JAR_FILE:$CLASSPATH

###################  check whether this is the first pod #############################
# if this is the first pod, HOSTNAME ends with "-0"
# in that case, it should run mpirun
# otherwise, it should start sshd and sleep infinity

echo "My hostname: $HOSTNAME"
length=${#HOSTNAME}
# echo "length of $HOSTNAME: $length"

lastTwoChars=$(echo $HOSTNAME| cut -c $(($length-1))-$length)
# echo "last two chars: $lastTwoChars"

if [ "$lastTwoChars" = "-0" ]; then
  echo "This is the first pod in the job: $HOSTNAME"
  echo "Starting $CLASS_TO_RUN .... "
  java $CLASS_TO_RUN
  echo "$CLASS_TO_RUN is done. Starting to sleep infinity ..."
  sleep infinity
else
  echo "This is not the first pod: $HOSTNAME"
  echo "Starting to sleep to infinity ..."
  sleep infinity
fi
