# Installation Instructions

This section explains how to install the required software to run
Twister2 jobs. We will first give the instructions
for using the Docker image prepared for standalone twister2 environment.
The second option is using a twister2-ready cluster on one of our systems, called Echo. We will explain how to use Echo systems to run your jobs.
We will start with a simple Hello World example and then continue with more complex example.

* [Docker image based installation](installation.md#docker-image-based-installation)
* [Using Echo cluster](installation.md#using-echo-cluster)

## Docker Image Based Installation

will be ready soon ....


## Using Echo Cluster

This option will utilize the already running Twister2 systems including the resource schedulers; Kubernetes and Mesos.
Please follow the instructions below to run jobs on Echo cluster.

Use SSH to login to following Echo machine by using the username and password given to you.

username@149.165.150.84

Twister2 is installed under twister2 directory.

### Running Hello World Example Using Kubernetes

Go to the directory:

*twister2/bazel-bin/scripts/package/twister2-0.1.0/*

In that directory, you will see a script file named;

*submit-k8s-job.sh*

This script runs the following command;

*./bin/twister2 submit kubernetes jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld*

Running that file will submit the HelloWorld job to Kubernetes. You can see the job at the dashboard:

http://149.165.150.81:8080/#/jobs

HelloWorld job starts 4 workers and completes after 1 minutes. After 1 minute, the job becomes COMPLETED if everything is fine.












