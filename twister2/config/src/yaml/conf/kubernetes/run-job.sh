#!/bin/bash

extra=1

if [ $# -ne "3" ]; then
  echo "Please provide following parameters: jobName numberOfWorkers numberOfJobs"
  exit 1
fi

jobName=$1
workers=$2
jobs=$3
echo "jobName: $jobName, workers: $workers, jobs: $jobs"

# generate jobID
jobID=$(java -cp examples/libexamples-java.jar:lib/libapi-utils-java.jar edu.iu.dsc.tws.examples.internal.rsched.GenJobID $jobName)

echo "jobID: $jobID"

# submit the job
bin/twister2 submit kubernetes jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.internal.rsched.BasicKubernetesJob $jobID $workers

########################################
# watch pods until all running
jobPods=$((workers + 1))

label="twister2-job-pods=t2pod-lb-${jobID}"
runningPods=$(kubectl get pods -l $label | grep Running | wc -l)

while [ $runningPods -ne $jobPods ]; do

  # sleep
  sleep 10

  # get number of Running pods
  runningPods=$(kubectl get pods -l $label | grep Running | wc -l)
  echo "Running Pods: $runningPods"
done

echo "All $runningPods pods are running."

# sleep some time for all worker logs to be ready
sleepTime=30
echo "Sleeping $sleepTime ............."
sleep $sleepTime

echo "Sleep finished. Getting logs....."

##############################################
# save log files
logDir=$HOME/t2-logs/${jobID}
mkdir ${logDir} 2>/dev/null

# copy jobSubmitTime
mv $HOME/.twister2/${jobID}-time-stamp.txt ${logDir}/jobSubmitTime.txt

# copy launch-delay.txt file
mv $HOME/.twister2/${jobID}-launch-delay.txt ${logDir}/launch-delay.txt

for ((i=0; i<workers ;i++)); do
  podName=${jobID}-0-${i}
  logFile=${logDir}/worker-${i}.log
  kubectl logs $podName > ${logFile}
  echo written logFile: ${logFile}
done

##############################################
# calculate delays and write to file

# create directory if not exist
mkdir delays 2>/dev/null

delayFile=delays/${jobID}.txt
java -cp examples/libexamples-java.jar edu.iu.dsc.tws.examples.internal.rsched.Delays $jobID > $delayFile

########################################
# watch pods until all pods running
allPods=$((jobPods * jobs + extra))

runningPods=$(kubectl get pods | grep Running | wc -l)

while [ $runningPods -ne $allPods ]; do

  # sleep
  sleep 10

  # get number of Running pods
  runningPods=$(kubectl get pods | grep Running | wc -l)
  echo "Running Pods: $runningPods"
done

echo "All $runningPods pods are running."

########################################
# sleep some time and kill the job

sleepTime=50
echo "Sleeping $sleepTime ............."
sleep $sleepTime

echo "Sleep finished. killing the job ......"

bin/twister2 kill kubernetes $jobID

########################################
# wait until all killed
runningPods=$(kubectl get pods | grep Running | wc -l)

while [ $runningPods -ne $extra ]; do

  # sleep
  sleep 10

  # get number of Running pods
  runningPods=$(kubectl get pods | grep Running | wc -l)
  echo "Running Pods: $runningPods"
done

echo "Only $runningPods pods are running."
