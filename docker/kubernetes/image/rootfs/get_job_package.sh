#! /bin/bash

# this script is used to get the job package to twister2 pods
# job package is either transferred directly into the pods by the job submitting client,
# or it is uploaded to a webserver
#
# If it is directly uploaded into the pods, this script (wait_job_package_to_arrive) waits for that package to arrive
# when it fully arrives, it unpacks it
#
# If it is uploaded to a web server, this script (get_job_package_with_wget) downloads the job package with wget
# and unpacks it
#
# There can be more than one container in a pods
# In that case, the first container in each pods gets the job package and unpacks it
# after they are done with getting the package and unpacking it, they write a flag file to inform other containers
# other containers just wait for the flag file by calling (wait_for_flag_file) method

# when the first container in a pod retrieves and unpacks the job package
# it creates this file and writes 1 into it
# other containers just wait for this file to be ready
FLAG_FILE="$POD_MEMORY_VOLUME/unpack-complete.txt"

# sleep interval for the waiting processes
SLEEP_INTERVAL=0.05

JOB_PACKAGE_FILE=$POD_MEMORY_VOLUME/$JOB_PACKAGE_FILENAME

###########################################################
# the method to wait for the job package to arrive
wait_job_package_to_arrive(){

  echo "Starting to wait for the job package to arrive ..."

  CURRENT_FILE_SIZE=-1
  if [ -f "$JOB_PACKAGE_FILE" ]; then
    CURRENT_FILE_SIZE=$(stat -c%s "$JOB_PACKAGE_FILE")
  fi

  while [ $JOB_PACKAGE_FILE_SIZE != $CURRENT_FILE_SIZE ]
  do
    sleep $SLEEP_INTERVAL

    if [ -f "$JOB_PACKAGE_FILE" ]; then
      CURRENT_FILE_SIZE=$(stat -c%s "$JOB_PACKAGE_FILE")
    fi
  done

  # unpack the job package
  tar -xf $JOB_PACKAGE_FILE -C $POD_MEMORY_VOLUME

  echo "1" >> $FLAG_FILE
  echo "Job package arrived fully to the pod."
  return 0
}

###########################################################
# the container except the first container in a pod just wait for the flag file to ge written
wait_for_flag_file(){

  echo "Waiting for the first container to get the job package file ..."

  while [ ! -f "$FLAG_FILE" ]
  do
    sleep $SLEEP_INTERVAL
  done

  echo "Job package file is ready ..."
}

###########################################################
# download the job package from the download directory and write the flag file
get_job_package_with_wget(){

  wget $DOWNLOAD_DIRECTORY/$JOB_PACKAGE_FILENAME -P $POD_MEMORY_VOLUME
  # check whether job package downloaded successfully
  if [ $? -ne 0 ]; then
    echo "Job package can not be retrieved from: $DOWNLOAD_DIRECTORY/$JOB_PACKAGE_FILENAME"
    return 1
  else
    echo "Job package downloaded successfully into the pod from: $DOWNLOAD_DIRECTORY/$JOB_PACKAGE_FILENAME"

    # unpack the job package
    tar -xf $JOB_PACKAGE_FILE -C $POD_MEMORY_VOLUME

    # write the flag file
    echo "1" >> $FLAG_FILE

    return 0
  fi
}

###########################################################
# main section of the script
# functions section finished
###########################################################

# if this is not the first container in a pod, wait for the flag file
# container name of the first container in a pod ends with -0
length=${#CONTAINER_NAME}
lastTwoChars=$(echo $CONTAINER_NAME| cut -c $(($length-1))-$length)
if [ "$lastTwoChars" != "-0" ]; then
  wait_for_flag_file
  exit 0
fi

# if it arrives to this point, it is the first container in the pod
if [ $UPLOAD_METHOD == "client-to-pods" ]; then
  wait_job_package_to_arrive
elif [ $UPLOAD_METHOD == "webserver" ]; then
  get_job_package_with_wget
else
  echo "Unrecognized upload method: $UPLOAD_METHOD"
  exit 1
fi

exit $?
