# Installation Instructions

This section explains how to install the required software to run
Twister2 jobs. We will first give the instructions
for using the Docker image prepared for standalone twister2 environment.
The second option is using a twister2-ready cluster on one of our systems, called Echo. We will explain how to use Echo systems to run your jobs.
We will start with a simple Hello World example and then continue with more complex example.

* [Using Docker Container](installation.md#using-docker-container)
* [Using Echo Cluster](installation.md#using-echo-cluster)

## Using Docker Container

We have created a Docker container for you to easily run jobs on twister2.
This container has all the necessary software installed in order to run
twister2 jobs using standalone resource scheduler.

Please follow the instructions below;
(You might need to add sudo in front of the docker commands below.)

First you need to pull the image;

* docker pull twister2tutorial/twister2:standalone

Then you need to run the container;

* docker run -it twister2tutorial/twister2:standalone bash

Now you should be in the docker container. Go into twister2-0.2.1 directory.

Run the following commands for related examples;

* Running hello world example;
  * ./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld
* Running batch word count example
  * ./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.wordcount.WordCountJob
* Running streaming word count example
  * ./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.streaming.wordcount.WordCountJob
* Running machine learning example K-Means
  * ./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.kmeans.KMeansJobMain -workers 4 -iter 2 -dim 2 -clusters 4 -fname /twister2-volatile/output.txt -pointsfile /twister2-volatile/kin
    put.txt -centersfile /twister2-volatile/kcentroid.txt -points 100 -filesys local -pseedvalue 100 -cseedvalue 200 -input generate


## Using Echo Cluster

This option will utilize the already running Twister2 systems including the resource schedulers; Kubernetes and Mesos.
Please follow the instructions below to run jobs on Echo cluster.

Use SSH to login to following Echo machine by using the username and password given to you.

username@149.165.150.84

Twister2 is installed under twister2 directory.

### Running Hello World Example Using Kubernetes

Go to the directory:

* twister2/bazel-bin/scripts/package/twister2-0.1.0/

In that directory, you will see a script file named;

* submit-k8s-job.sh

This script runs the following command;

* ./bin/twister2 submit kubernetes jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld

Running that file will submit the HelloWorld job to Kubernetes. You can see the job at the dashboard:

http://149.165.150.81:8080/#/jobs

HelloWorld job starts 4 workers and completes after 1 minutes. After 1 minute, the job becomes COMPLETED if everything is fine.












