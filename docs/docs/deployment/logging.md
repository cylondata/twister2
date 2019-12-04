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
These log files are shown to users through Dashboard application. 
Users can download the logs of their applications to their local machines through Dashboard web interface.

Users can also get logs by using kubectl command. First, they need to learn the pod names for workers. 
You can execute "kubectl get pods" command to list the pods in the cluster. 
The pod names for the workers are in the form of <job-name><ss-index><pod-index>.
You can see the logs of each pod by executing the command: 

```bash
    $ kubectl logs <podname>
```
