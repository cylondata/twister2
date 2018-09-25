Submitting Jobs in Kubernetes
=============================

We assume that you have a running Kubernetes cluster. 
In your machine, kubectl is configured to talk to Kubernetes master. 

Running HelloWorld Example
-------------------------- 

You can submit jobs to Kubernetes cluster by using twister2 executable: 

    bin/twister2

When submitting jobs to Kubernetes clusters, you need to specify the cluster name as "kubernetes".
You can submit HelloWorld job in examples package with 8 workers as:

    ./bin/twister2 submit kubernetes jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8

If there is a problem with job submission, job submission client will exit with a coresponding message. 
Otherwise, job submission client finishes execution with success. 
Job is submitted to Kubernetes cluster. 

Job Outputs
-----------
We can see the job output either from Kubernetes Dashboard website or persistent logs. 
The workers in HelloWorld job prints a log message and sleeps 1 minutes before exiting.
So the user can check Kubernetes Dashboard website for StatefulSets. 
There must be a StatefulSet with the job name. 
List the pods in that StatefulSet. 
Check the output for each pod by clicking on the right hand side button. 
You will see the output for each worker over there. 

You can also check the log files from persistent storage. 
But first, you need to have a persistent storage provisioner in your cluster. 
Then, you need to specify the storage class in configuration file:

    kubernetes.persistent.storage.class

Second, learn the persistent logging directory of your storage provisioner. 
You can learn it from Kubernetes Dashboard by checking the provisioner entity or 
consulting your administrator. Check that directory for job logs. 

Terminating a Running Job
-------------------------

While some jobs automatically complete when they finish execution (ex: batch jobs).
Some other jobs may continually run (ex: streaming jobs). Some jobs may also stuck or
take a long time to finish. If we want to terminate a running job, we can use twister2 command
with the job name: 

    ./bin/twister2 kill kubernetes job-1

This command kills the job with the name: "job-1"

Configuration Settings
----------------------

Configuration files for kubernetes clusters are under the directory: 

    conf/kubernetes/

You can specify job related configurations either through client.yaml file or
in your Job java file. For example job name can be specified in both locations. 
Java file has precedence of the conf files if you specify in both locations. 