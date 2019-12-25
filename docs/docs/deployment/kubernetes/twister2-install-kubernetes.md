---
id: Kubernetes
title: Kubernetes
sidebar_label: Kubernetes
---

We assume that Kubernetes cluster is already installed and kubectl is configured. 
To install Twister2 on Minikube, please follow the steps at: 
[Installing Twister2 on Minikube](../minikube/twister2-install-minikube.md)

To install and compile Twister2 project on your machine, please follow the steps in [compiling document](../../compiling/compiling.md).
You may also check [developer document](../../developers/developer-environment.md) for setting up IDEs.

To run Twister2 jobs on Kubernetes clusters, You must perform the following step: 
* [**Authorization of Pods**](#authorization-of-pods).

Here are the steps to run jobs in Kubernetes clusters:
* [**Running Jobs in Kubernetes**](#running-jobs-in-kubernetes)

Here are the list of optional installations/settings: 
* [**Deploying Twister2 Dashboard**](#deploying-twister2-dashboard): This is a recommended step to monitor Twister2 jobs on Kubernetes clusters. 
* [**Persistent Storage Settings**](#persistent-storage-settings): You can set up persistent storage only if you want to use. 
* [**Fault Tolerant Jobs**](#fault-tolerant-jobs): A ZooKeeper server is required, if you want to run fault-tolerant Twister2 jobs. 
* [**Generating Secret Object for OpenMPI Jobs**](#generating-secret-object-for-openmpi-jobs): This is required only if you are going to run OpenMPI jobs. 
* [**Providing Rack and Datacenter Information**](#providing-rack-and-datacenter-information-to-twister2): This is required only if you want Twister2 to perform rack and data center aware scheduling. 
* [**Job Package Uploader Settings**](#job-package-uploader-settings): This is required only if you want to upload the job package through a web server. 

Twister2 runs jobs in Docker containers in Kubernetes clusters. 
Developers need to rebuild Twister2 Docker image if they want to modify twister2 source codes: 
* [**Building Twister2 Docker Image for Kubernetes**](#building-twister2-docker-image-for-kubernetes)

**Requirements**: 
* Twister2 requires Kubernetes version v1.10 or higher.  
* When ZooKeeper is used, at least ZooKeeper version 3.5 or above is required. 

## Authorization of Pods

Twister2 Worker pods need to watch Job Master pod and get its IP address. 
In addition, Job Master needs to be able to scale worker pods during the computation 
and delete job resources after the job has completed. 
Therefore, before submitting a job, a Role and a RoleBinding object need to be created. 
We prepared the following YAML file: twister2-auth.yaml.

If you are using "default" namespace, then execute the following command: 

```bash
    $ kubectl create -f https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/twister2/config/src/yaml/conf/kubernetes/deployment/twister2-auth.yaml
```

If you are not using "default" namespace, download the above yaml file, 
change the namespace field value to the namespace value that your users will use to submit Twister2 jobs. 
Then, execute the following command:

```bash
    $ kubectl create -f /path/to/file/twister2-auth.yaml
```

## Running Jobs in Kubernetes

You can submit and kill jobs in Kubernetes as it is explained in the [job submission document](../job-submit.md).
You must specify the cluster type as "kubernetes". 

If there is a problem with job submission, job submission client will exit with a corresponding message 
printed to the screen. Otherwise, job submission client either finishes execution with success or 
waits to upload the job package to workers.

### Job Logs

You can see the job logs either from Kubernetes Dashboard website, using kubectl command or 
through persistent storage logs if enabled. 
The workers in HelloWorld job prints a log message and sleeps 1 minutes before exiting. 
So the user can check Kubernetes Dashboard website for worker log messages. 
There must be a StatefulSet with the job name. List the pods in that StatefulSet. 
Check the output for each pod by clicking on the right hand side button. 
You will see the output for each worker in that window.

To see the logs by using kubectl, you first need to learn the pod names in the job. 
You can execute "kubectl get pods" command to list the pods in the cluster. 
The pod names for the job are in the form of <job-name><ss-index><pod-index>.
You can see the logs of each pod by executing the command: 
```bash
    $ kubectl logs <podname>
```

You can also check the log files from persistent storage if the persistent storage is enabled. 
You need to learn the persistent logging directory of your storage provisioner. 
You can learn it from Kubernetes Dashboard by checking the provisioner entity or consulting your administrator. 
Check that directory for the job logs.

### Logs for MPI Enabled Jobs

The outputs of MPI enabled jobs are a little different in Kubernetes Dashboard and pod logs. 
In those jobs, first worker starts all other workers by using mpirun command. 
Therefore, the outputs of all other pods are transferred to the first pod.  
So, all outputs from all workers in an MPI enabled job is shown in first worker pod. 
The pods of other workers will only have initialization messages.

However, persistent log files are different. In those files, each worker has its own log messages in its log file. 
Therefore, checking log files would be more preferable in MPI enabled jobs compared to checking the dashboard.

### Configuration Settings

Configuration files for kubernetes clusters are under the directory:

```text
conf/kubernetes/
```

You can specify job related configurations either through resource.yaml file or in your job java file 
by using Twister2Job class methods. 
If you specify job parameters in resource.yaml file, then you can load them by using following method in your code: 
```text
Twister2Job.loadTwister2Job()
```

### Job Names

We are using job names as StatefulSet and Service names. In addition we are using job names as labels. 
Therefore job names must follow Kubernetes naming rules: [Kubernetes resource naming rules](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/).

Job names should consist of lower case alphanumeric characters and dash\(-\) only. Their length can be 50 chars at most. 
If job names do not conform to these rules, we automatically change them to accommodate those rules. 
We use the changed names as job names.  

## Deploying Twister2 Dashboard

Twister2 Dashboard enables users to monitor their jobs through a web browser. 
Although installing this dashboard is not mandatory for running Twister2 jobs, it is highly recommended. 
A single instance of the Dashboard runs in each cluster for all users. 

If you are using default namespace, then you can deploy Twister2 Dashboard without persistent storage as:

```bash
    $ kubectl create -f https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/twister2/config/src/yaml/conf/kubernetes/deployment/twister2-dashboard-wo-ps.yaml
```

If you are using another namespace, or would like to change a parameter of Dashboard, 
then you can download this file, change the desired parameter values and 
execute the create command on the modified file. 

### Accessing Dashboard

You can access the Dashboard from any machine that has kubectl installed and 
has the credentials to connect to the kubernetes cluster. 

First run the following command in your workstation to create a secure channel: 

```bash
    $ kubectl proxy
```

Then, access Dashboard at the following URL:

```text
    http://localhost:8001/api/v1/namespaces/default/services/http:twister2-dashboard:/proxy/#/
```

If you are using a namespace other than the default, replace "default" in the URL with your namespace value. 

### Running Dashboard as a standalone web server

You can also run Dashboard as a standalone webserver in your cluster. 
Job master pod should be able to access the machine that is running Dashboard to be able to feed job status data. 

Run Dashboard server with the following command: 

```bash
    $ bin/twister2 dash
```

Dashboard runs at the port 8080. 

You need to give the address of Dashboard at the configuration file: conf/kubernetes/core.yaml 
You should set the value of following parameter:  

```text
    twister2.dashboard.host: "http://<host-address>:8080"
```

## Persistent Storage Settings

To enable persistent storage in Twister2, a StorageClass must exist in the cluster backed by a dynamic storage provisioner. 
The name of the persistent StorageClass needs to be specified in the conf/kubernetes/resource.yaml configuration file. 
Configuration parameter is:

```text
    twister2.resource.kubernetes.persistent.storage.class
```

We tested with NFS-Client provisioner from: [https://github.com/kubernetes-incubator/external-storage/tree/master/nfs-client](https://github.com/kubernetes-incubator/external-storage/tree/master/nfs-client)
We also tested with provisioners in AWS and Minikube. 

## Fault Tolerant Jobs

We use ZooKeeper servers to save job meta state in fault tolerant jobs. 
In the case of failures, job master and workers get the job meta data from zookeeper servers and rejoin. 

ZooKeeper servers are not used in non-fault tolerant Twister2 jobs. 

Following configuration parameter needs to be set as true to specify the job as fault tolerant in core.yaml: 

```text
twister2.fault.tolerant
```

The addresses of ZooKeeper servers are specified with the configuration parameter in resource.yaml. 
If there are multiple zookeeper servers, they can be specified in the form of "ip1:port,ip2:port,ip3:port"

```text
twister2.resource.zookeeper.server.addresses
```

If you do not already have a ZooKeeper server running, you can run an experimental one by deploying it 
as a pod. We have a yaml file for that. You can deploy it as: 

```bash
$ kubectl create -f https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/twister2/config/src/yaml/conf/kubernetes/deployment/zookeeper-wo-persistence.yaml
```

Then you can get its ip address by using following command and specify that in above resource.yaml file. 
It runs at the port 2181.  
```bash
$ kubectl get pods -o=wide | grep twister2-zookeeper
```

**Requirement**: Twister2 requires at least ZooKeeper version of 3.5. 

## Generating Secret Object for OpenMPI Jobs

When using OpenMPI communications in Twister2, pods need to have password-free SSH access among them. 
This is accomplished by first generating an SSH key pair and deploying them as a Kubernetes Secret on the cluster.

First, generate an SSH key pair and save them in files by using:

```bash
    $ ssh-keygen
```

Second, create a Kubernetes Secret object for the namespace of Twister2 users with the already generated key pairs. 
Execute the following command by specifying generated key files. Last parameter is the namespace. 
If you are using a namespace other than default, please change that. 

```bash
    $ kubectl create secret generic twister2-openmpi-ssh-key --from-file=id_rsa=/path/to/.ssh/id_rsa --from-file=id_rsa.pub=/path/to/.ssh/id_rsa.pub --from-file=authorized_keys=/path/to/.ssh/id_rsa.pub --namespace=default
```

The fifth parameter \(twister2-openmpi-ssh-key\) is the name of the Secret object to be generated. 
That has to match the following configuration parameter in the network.yaml file:

```text
    kubernetes.secret.name
```

You can retrieve the created Secret object in YAML form by executing the following command:

```bash
    $ kubectl get secret <secret-name> -o=yaml
```

Another possibility for deploying the Secret object is to use the [YAML file template](https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/docs/architecture/resource-schedulers/kubernetes/yaml-templates/secret.yaml). You can edit that secret.yaml file. You can put the public and private keys to the corresponding fields. You can set the name and the namespace values. Then, you can create the Secret object by using kubectl method as:

```bash
    $ kubectl create secret -f /path/to/file/secret.yaml
```

## Providing Rack and Datacenter Information to Twister2

Twister2 can use rack names and data center names of the nodes when scheduling tasks. 
There are two ways administrators and users can provide this information.

**Through Configuration Files**:  
Users can provide the IP addresses of nodes in racks in their clusters. 
In addition, they can provide the list of data centers with rack data in them.

Here is an example configuration:

```text
    kubernetes.datacenters.list:
    - dc1: ['blue-rack', 'green-rack']
    - dc2: ['rack01', 'rack02']

    kubernetes.racks.list:
    - blue-rack: ['node01.ip', 'node02.ip', 'node03.ip']
    - green-rack: ['node11.ip', 'node12.ip', 'node13.ip']
    - rack01: ['node51.ip', 'node52.ip', 'node53.ip']
    - rack02: ['node61.ip', 'node62.ip', 'node63.ip']
```

Put these lists to resource.yaml file. Then, assign the following configuration parameter in resource.yaml as true: 

```text
    kubernetes.node.locations.from.config
```

**Labelling Nodes With Rack and Data Center Information**:  
Administrators can label their nodes in the cluster for their rack and datacenter information. 
Each node in the cluster must be labelled once. When users submit a Twister2 job, 
submitting client first queries Kubernetes master for the labels of nodes. 
It provides this list to all workers in the job.

**Note**: For this solution to work, job submitting users must have admin privileges.

**Example Labelling Commands**: Administrators can use kubectl command to label the nodes in the cluster. 
The format of the label creation command is as follows:

```text
    $ kubectl label node <node-name> <label-key>=<label-value>
```

Then, used rack and data center labels must be provided in the configuration files. 
These configuration parameters are:

```text
    rack.labey.key
    datacenter.labey.key
```

To get the rack and datacenter information from Kubernetes master using labels, 
the value of the following configuration parameter has to be specified as false in client.yaml file: 

```text
    kubernetes.node.locations.from.config
```

## Job Package Uploader Settings

When users submit a Twister2 job in Kubernetes cluster, 
submitting client needs to transfer the job package to workers and the job master. 
The submitting client first packs all job related files into a tar package file. 
This archive file needs to be transferred to each worker that will be started.

We provide [two methods](../../architecture/resource-schedulers/kubernetes/twister2-on-kubernetes.md):

* Job package file transfer from submitting client to job pods directly
* Job package file transfer through uploader web server pods

We first check whether there is any uploader web server running in the cluster.
If there is, we upload the job package to the uploader web server pods. 
Job pods download the job package from uploader pods. 
Otherwise, submitting client uploads the job package to all pods in the job directly. 
Both methods transfer the job package from client to pods by using kubectl copy method.

If you are running many Twister2 jobs with many workers, 
it would be helpful to run uploader web server pods. 
It is more efficient and faster. 
We designed a StatefulSet that runs an nginx web server. 
You can deploy it with following command: 

```bash
$ kubectl create -f https://raw.githubusercontent.com/DSC-SPIDAL/twister2/master/twister2/config/src/yaml/conf/kubernetes/deployment/twister2-uploader-wo-ps.yaml
```

You can modify the number of replicas in uploader web server or 
the compute resources used in the above yaml file. 

## Building Twister2 Docker Image for Kubernetes

First, build Twister2 project. 

This will generate the twister2 package file: 

bazel-bin/scripts/package/twister2-0.4.0.tar.gz

While on twister2 main directory, unpack this tar file:

```bash
$ tar xf bazel-bin/scripts/package/twister2-0.4.0.tar.gz
```

This will extract the files under twister2-0.4.0 directory. 

Build Docker image by running the following command from twister2 main directory. 
Update username with your Docker Hub user name. 

```bash
$ docker build -t <username>/twister2-k8s:0.4.0 -f docker/kubernetes/image/Dockerfile .
```

Push generated Docker image to Docker Hub. If you have not already logged in to Docker Hub account, 
please login first with command: "docker login docker.io" 
```bash
$ docker push <username>/twister2-k8s:0.4.0
```

Update Docker Image name in the following resource.yaml file. 
Put new Docker Image name as the value of the key: "twister2.resource.kubernetes.docker.image" 
```bash
$ nano twister2/config/src/yaml/conf/kubernetes/resource.yaml
```

You may also want to update the value of the following key to "Always" in the same "resource.yaml" 
file: 
````
kubernetes.image.pull.policy: "Always"
````
 
With this, docker will pull the twister2 image from Docker Hub at each run.  
This may slow down startup times for the jobs. 
However, if you repeatedly build the docker image, this may be convenient.
