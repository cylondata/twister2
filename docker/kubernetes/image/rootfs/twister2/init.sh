#! /bin/bash
####################################################################
# This script starts a Twister2 worker or Job Master in jobs
# It:
#   gets the job package and unpacks it, if it is the first worker in a pod
#   if it is not the first worker,
#   it waits for the first worker to get the job package and unpack it
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
CLASSPATH=$POD_MEMORY_VOLUME/$JOB_ARCHIVE_DIRECTORY/$USER_JOB_JAR_FILE:/twister2/lib/ucx/:$CLASSPATH
LOGGER_PROPERTIES_FILE=$POD_MEMORY_VOLUME/$JOB_ARCHIVE_DIRECTORY/$LOGGER_PROPERTIES_FILE

# start the class to run
echo "Starting $CLASS_TO_RUN .... "
java -Xms${JVM_MEMORY_MB}m -Xmx${JVM_MEMORY_MB}m \
  -Djava.util.logging.config.file=$LOGGER_PROPERTIES_FILE \
  $CLASS_TO_RUN

result=$?

if [ $result -eq 0 ]; then
  echo "$CLASS_TO_RUN completed. Waiting the pod to be deleted."
  sleep infinity
else
  echo "$CLASS_TO_RUN has not completed successfully. return code: $result"
  # return the exit code of twister2-worker or jm,
  # so that they will be restarted in case of failure
  exit $result
fi

