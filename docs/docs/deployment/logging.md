---
id: logging
title: Logging
sidebar_label: Logging
---

We use a Java properties file to set logging properties for Twister2. 
All Twister2 entities use the same file including Twister2 client, workers and the job master. 
The file name is: 

```text
common/logger.properties
```

Users can set logger levels for packages.  
For example, org.apache.zookeeper logger level is set to WARNING by default in that file. 
Probably, you may want to set twister2 library logging level to more than INFO. 
Because, it tends to log many messages in INFO. It may make debugging your applications difficult. 
So, you can set it to WARNING as following: 
```text
edu.iu.dsc.tws.level=WARNING
```

If you enabled persistent storage, then you can get log messages to be saved in the log files in persistent storage. 
You just need to make sure that edu.iu.dsc.tws.common.logging.Twister2FileLogHandler exist as a handler in
logger.properties file. 

We save all log files in a job to a directory named logs. 
A log file is created for each worker in this directory. Worker log files are named as:

```text
  * worker-0.log.0
  * worker-1.log.0
  * worker-2.log.0
  * ...
```

There may be more than one log file for each worker. 
In that case, log files are created with increasing suffix values.

### Logging in Kubernetes  
Logging in distributed environments is an important task. Cluster resource schedulers provide logging mechanisms. 
Kubernetes saves applications logs in files under /var/log directory in agent machines. 
These log files are shown to users through Kubernetes Dashboard application. 
Users can download the logs of their applications to their local machines through Dashboard web interface.

Users can also get logs by using kubectl command. First, they need to learn the pod names for workers. 
You can execute "kubectl get pods" command to list the pods in the cluster. 
The pod names for the workers are in the form of <job-name><ss-index><pod-index>.
You can see the logs of each pod by executing the command: 

```bash
    $ kubectl logs <podname>
```

There are a number of ways to check the logs of a Twister2 job at Kubernetes clusters. 
* Saving Log Files in Submitting Client
* Saving Log Files in Persistent Storage
* Getting Log Files from Kubernetes Dashboard
* Getting Log Files with kubectl

#### Saving Log Files in Submitting Client
Before submitting the job, users need to set the following parameter to true in the config file 
conf/kubernetes/resource.yaml: 
```text
kubernetes.log.in.client 
```

Log files of all workers and the job master will be saved to the directory: 
```text
$HOME/.twister2/jobID
```

Loggers will run in the submitting client as threads. So, you should let the submitting client run during
the job execution. 

#### Saving Log Files in Persistent Storage
When a persistent storage is enabled for a job, 
log files are written to that directory by default. 
They are saved in the directory called "logs" under the persistent storage directory. 

You need to learn the persistent logging directory of your storage provisioner. 
You can learn it from Kubernetes Dashboard by checking the provisioner entity or consulting your administrator. 

#### Getting Log Files from Kubernetes Dashboard
Kubernetes saves applications logs in files under /var/log directory in agent machines. 
These log files are shown to users through Kubernetes Dashboard application. 
Users can see and download the logs of their applications to their local machines through Dashboard web interface.

Go to Kubernetes Dashboard website. 
There must be at least two StatefulSets with the jobID. 
One of them is for the job master and the other(s) is for workers. 
List the pods in a StatefulSet. 
Check the output for each pod by clicking on the right hand side button. 
You will see the output for each worker in that window.

#### Getting Log Files with kubectl
Users can also get logs by using kubectl command.
Users first need to learn the pod names in the job. 
You can execute "kubectl get pods" command to list the pods in the cluster. 
The pod names for the job are in the form of <username><job-name><timestamp><ss-index><pod-index>.
You can see the logs of each pod by executing the command: 
```bash
    $ kubectl logs <podname>
```

### Logs for MPI Enabled Jobs

The logs of MPI enabled jobs are a little different. 
In those jobs, first worker starts all other workers by using mpirun command. 
Therefore, the outputs of all other pods are transferred to the first pod.  
So, all outputs from all workers in an MPI enabled job is shown in the first worker log. 
The pods of other workers will only have initialization messages.

However, persistent log files are different. In those files, each worker has its own log messages in its log file. 
Therefore, checking log files would be more preferable in MPI enabled jobs compared to checking the dashboard.
