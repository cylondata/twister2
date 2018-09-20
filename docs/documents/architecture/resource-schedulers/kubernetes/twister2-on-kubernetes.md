Twister2 on Kubernetes: Design, Features and Implementation
===========================================================

This document explains the design, features and implementations for running Twister2 jobs 
on Kubernetes clusters. We designed and developed components to run Twister2 workers and
the Job Master in Kubernetes clusters. An overview of the mian components of Twister2 
architecture can be found in [the document](../../main-components.rst).

First, we provide a general overview of Kubernetes system. We discuss its features from 
Twister2 implementation perspective. Then, we explain the design decisions that we have made 
when implementing each aspect of Twister2 on Kubernetes. We provide the implementation details 
and explain the features. In addition, we explain the limitations of each feature 
and point out the future works.

We discuss the following topics:
* [Kubernetes Overview](#kubernetes-overview)
* [Implementing Twister2 Jobs on Kubernetes](#implementing-twister2-jobs-on-kubernetes)
* [Docker Container Design for Twister2](#docker-container-design-for-twister2)
* [Kubernetes Pod Design for Twister2](#kubernetes-pod-design-for-twister2)
* [Packaging and Running Twister2 Jobs in Kubernetes](#packaging-and-running-twister2-jobs-in-kubernetes)
* [Limitations and Future Works](#limitations-and-future-works)
* [Kubernetes Objects for Twister2 Jobs](#kubernetes-objects-for-twister2-jobs)

In addition to running Twister2 jobs on Kubernetes clusters, We have implemented 
the following APIs/services: 
* [Worker Discovery](k8s-based-worker-discovery.rst)
* [Persistent Storage](k8s-persistent-storage.rst)
* [Services for Twister2 Jobs](k8s-services.rst) 
* [Worker Binding and Mapping](k8s-worker-binding-mapping.rst)
* [OpenMPI Support](open-mpi-support.rst)

Each one is explained in a separate document. 

## Kubernetes Overview
Kubernetes is an open source system for managing containerized applications across multiple hosts, 
providing basic mechanisms for deployment, maintenance, and scaling of applications.

Kubernetes provides two types of computing components: 
* pods 
* containers

### Pods
Pods are like virtual machines. They run containers. The applications in containers perform 
the actual computation. A pod without a container is useless.
 
Pods have: 
* a file system that can be shared by the running containers in the pod
* an IP address that can be used by the running containers in the pod

### Containers
Containers are packaged applications with all their dependencies. 
They are supposed to run without any outside dependency.

Containers run in pods in Kubernetes. There can be one or multiple containers running on a pod. 

**Container to Container Communications**  
Containers use the IP address of the encompassing pod to communicate with other containers. 
Containers have their own port numbers though.
 
Containers can communicate with other containers in the same pod with shared files 
or localhost network interface.
 
Containers can communicate with other containers in other pods using IP communication 
or mounting a shared files system among the pods such as NFS. 

### Containers and Pod Management
Kubernetes provides services to manage containerized applications for deployment, maintenance, and scaling:
* It can restart failed containers. 
* It can increase the number of instances running or decrease them (scale up - scale down). 
* It can help rolling updates to applications. 
* It provides monitoring tools for pods and containers. 
* It provides networking facilities. It provides internal IP addresses to each pod. 
It can also expose pod applications to outside world by setting up services. 
* It provides pod binding and mapping to nodes in clusters. 
Users can request their pods to run in some specific nodes or vice versa. 

Kubernetes provides many types of pod management constructs. Some of them are 
* Deployments
* StatefulSets
* Jobs
* Bare pods

#### Deployments
A deployment object tries to bring back the system to its desired state at a controlled rate. 
It can make sure that some number of replicas for pods always run. It can be used for:
* A new state can be defined and the system can be moved to the new state at a controlled rate.
* It can rollback to an earlier Deployment revision if the current state of the Deployment is not stable. 
* Scale up the Deployment to facilitate more load. 
* Pause the Deployment to apply multiple fixes to its PodTemplateSpec and then resume it to start a new roll out.

#### StatefulSets
StatefulSet is used to manage stateful applications. 
StatefulSets are valuable for applications that require one or more of the following.
* Stable, unique network identifiers.
* Stable, persistent storage.
* Ordered, graceful deployment and scaling.
* Ordered, graceful deletion and termination.
* Ordered, automated rolling updates.

**Unique IDs for pods**: a StatefulSet maintains a sticky identity for each of their Pods. 
These pods are created from the same spec, but are not interchangeable: 
each has a persistent identifier that it maintains across any rescheduling.

**Persistent Storage**: The storage for a given Pod must either be provisioned by a PersistentVolume 
Provisioner, or pre-provisioned by an admin. Deleting a StatefulSet/pod will not delete the volumes 
associated with the StatefulSet. Deleting volumes must be done manually.
 
**Requirement**: StatefulSets currently require a Headless Service to be responsible 
for the network identity of the Pods. You are responsible for creating this Service.

**Ordinal Index**: For a StatefulSet with N replicas, each Pod in the StatefulSet will be assigned 
an integer ordinal, from 0 up through N-1, that is unique over the Set.

**Pod Initiation Policy**: Pods of a StatefulSet can be initiated 
in order (one after the other) or in parallel. 

#### Jobs
A job creates one or more pods and ensures that a specified number of them successfully 
terminate. As pods successfully complete, the job tracks the successful completions. 
When a specified number of successful completions is reached, the job itself is complete. 
Deleting a Job will cleanup the pods it created.

**Parallel Jobs with a fixed completion count**: the job is complete when a desired 
number of pods complete with success. Pods are restarted on unsuccessful exits.
 
**Parallel Jobs with a work queue**: Each pod is independently capable of determining 
whether or not all its peers are done, thus the entire Job is done. 
When any pod terminates with success, no new pods are created. 
Once any pod has exited with success, no other pod should still be doing any work or 
writing any output. They should all be in the process of exiting 
(This means that pods should wait others to complete before exiting).

#### Bare Pods
Bare pods are fully managed by users. They are initialized and ended by users. 
Kubernetes does not provide any services. Bare pods have their own cluster IP addresses. 
They can communicate with other pods in the cluster by using this IP address. 

## Implementing Twister2 Jobs on Kubernetes
We decided to use StatefulSets for implementing Twister2 jobs on Kubernetes:
* They provide unique IDs for all pods in a job
* They restart the failed pods with the same id
* They provide persistent storage support
* They support services for external communications

Disadvantages of StatefulSets:
We need to monitor pod completions and delete the StatefulSet objects explicitly 
when all workers have completed. When a pod exits without being deleted, 
Kubernetes tries to restart it. So when pods have completed, they need to wait to be deleted. 

Initialization costs of StatefulSets are a little longer than the initialization costs of Jobs. 
My tests shows that it takes 5-10% more time to initialize a StatefulSet compared to a Job. 

Heron is implemented by using StatefulSets. 

### Implementing Twister2 Workers in Kubernetes
There are a number of choices when implementing Twister2 workers in Kubernetes:
- **One worker one pod**: Exactly one worker runs in each pod. Each pod runs a single container. 
- **One worker one container**: Exactly one worker runs in each container but multiple containers may run in a pod.
Therefore, multiple workers may run in a pod as separate containers.
- **One worker one process**: Multiple workers can run in a container as separate processes.

One worker one pod is the recommended solution for services/applications to run in Kubernetes. 
However, initialization of a separate pod for each worker is slower. 

Kubernetes does not provide any support (logging, monitoring) for multiple processes 
in a container. We need to keep track of the lifecycle of processes in containers. 
Also no resource isolation is provided. 

Second option seems to be the best option for our case. Each Twister2 worker runs 
as a container. Multiple workers can run in a single pod. 
Users can set the number of workers in each pod using a configuration file. 
This also covers the first option since users can request that each pod runs only one worker. 

**When running OpenMPI**: When we execute OpenMPI jobs, then we go with the third option. 
We start a single container in each pod, then OpenMPI starts MPI worker processes. 
It can start more than one MPI worker in a container. 
OpenMPI manages the processes in this case. 

Heron runs one pod for each Heron container. They go with the first option. 
(Ref: https://github.com/twitter/heron/blob/master/website/content/docs/operators/deployment/schedulers/kubernetes.md)

### StatefulSet for Twister2 Workers
We create Twister2 workers in a job by creating a StatefulSet in Kubernetes cluster. 
For a non-MPI job, we create one container for each worker. 
The user specifies the number of containers (workers) in a pod through the configuration parameter:

    kubernetes.workers.per.pod

This parameter shows the number of containers to start in each pod. 
Since all pods are identical in StatefulSets, all pods in Twister2 jobs must also be identical.
Therefore, all pods must have the same number of Twister2 workers.

The user also specifies the total number of workers in job through the configuration parameter:

    twister2.worker.instances

We divide the total number of workers to the workers per pod value 
to get the number of pods in a job. The value of twister2.worker.instances must be divisible 
by the value of kubernetes.workers.per.pod. Otherwise, we reject the job submission. 

**Resources in OpenMPI**: When an OpenMPI enabled job is submitted, 
one container is started in every pod.
When more than one worker will run in each pod, then this one container 
has the resources of all workers in that pod. For example, if 3 workers will run
in each pod and each worker requests 1 CPU, then this one container will have 3 CPUs.
RAM is also similarly allocated to the single container in the pod. 

mpirun command will start the workers in each pod. When mpirun is executed,
we give the number of workers to start in each pod as a parameter. 
Therefore, the requested number of workers will be started in each pod. 
But, all workers will run in the same container in a pod when OpenMPI is used. 

#### Pod Names and Pod IP Addresses
We set job names as StatefulSet names. Kubernetes sets pod names as StatefulSet names
with an index attached. For example, if the job name is "basic-kube", 
then the pod names in the job will be: "basic-kube-0", "basic-kube-1", "basic-kube-2",
"basic-kube-3", etc. Pod names start with the suffix "-0" and increase sequentially. 
This pod naming helps us identifying the pods in a job. 

For example, when we start an OpenMPI enabled job, we run mpi master in the first pod.
We determine the first pod in a job by examining its suffix. The first pod name always
ends with the suffix "-0". 

Each pod is also assigned a unique cluster local IP address. 
We query the localhost from inside the pod and get this IP address.

#### Worker Ports in Non-MPI Jobs
We assume that each Twister2 worker has at least one port number to talk to other workers in a job. 
When we create non-MPI jobs, each worker runs in one container. 
Therefore, when creating containers we set one port number for the worker in that container. 

Since each pod is created solely for twister2 jobs, we can use whichever port we want 
from that pod port space. The only limitation is that when there are multiple workers in a pod, 
they can not use the same port numbers. They need to use separate port numbers.

We specify a base port number to be used by workers. It is defined in the configuration files as: 

    kubernetes.worker.base.port

First worker in a pod uses the base port number. Second worker in that pod uses the port (basePort +1). 
Third worker in that pod uses the port (basePort +2). 
Likewise, we assign sequentially increasing port numbers to workers running on a pod.

Container name suffix and the port number addition value to the base port is the same.
For example, the third container name suffix in a pod is "-2" and the port number is basePort + 2. 
We uses this parallelism between container names and port numbers 
when discovering port numbers from container names.

#### Worker Ports in MPI Jobs
When we create MPI enabled Twister2 jobs, each container may run multiple workers. 
Therefore, when creating containers we do not set port numbers for these containers. 

MPI enabled Twister2 workers will use the port based on their MPI ranking. 
They use the port: BasePort + MPI Ranking

Since MPI ranking is unique in a job, no two workers will use the same port.   

### StatefulSet for Job Master
We create a separate Kubernetes StatefulSet for the Job Master of the submitted job. 
Since, Job Master resource needs and worker resource needs are different. 
Job Master needs to be started as a separate pod in Kubernetes
with its own CPU, memory and volume requirements. 

In addition, we do not transfer the job package to Job Master pod. 
We only transfer the necessary information as environment variables. 

## Docker Container Design for Twister2
We designed a Docker container to run Twister2 jobs. 
We installed the following software: 
* based on ubuntu 16.04.
* Java 8
* OpenMPI 3.0.0 with Java
* OpenSSH
* Twister2 dependency library files
* Twister2 scripts and library files 

Twister2 Docker container will be pushed to a container hub such as Docker hub. 
It will be downloaded from there to user clusters. 

The working directory for the Twister2 jobs is set as:  

    /twister2

## Kubernetes Pod Design for Twister2
We designed a Kubernetes pod to run Twister2 jobs in Twister2 Docker containers.
Twister2 pods will have password free SSH among them when running OpenMPI jobs. 

Twister2 pods will have the following volumes.

### Pod Memory Volume
Each Twister2 pod will have a memory based directory 
that will be shared among the containers (workers) in that pod.
It is supposed to be fast but not large in size. 
It uses the capacity of the assigned pod memory. 
The job package is saved to this directory to share among containers in the pod. 
Volume is mounted as: 

    /twister2-memory-dir

### Pod Volatile Volume
An optional volatile volume is provided to each pod, 
if the user enables it from the configuration files. Configuration parameter is 

    twister2.worker.volatile.disk

This volume will be deleted after the worker completes.
the data is saved in the disk of the local machine and deleted after the pod is deleted. 
It is supposed to be used for intermediate data storage during the job. 
This volume is shared among the containers in each pod. 
The volatile volume is mounted as: 

    /twister2-volatile 

### Persistent Shared Volume
An optional persistent volume is provided to each pod, 
if the user enables it from the configuration files. Configuration parameter is 

    persistent.volume.per.worker

This volume is created on a shared file system such as NFS.
For Twister2 to configure a persistent volume, a Persistent Storage Provisioner 
or statically configured PersistentVolume must exist in the cluster.
The content of this volume will be available after the job has completed. 
While the memory volume and volatile volume is shared among only the containers in a pod, 
this volume is shared by all pods and all workers in a job.
Therefore, this volume can be used for both durable data and 
sharable data among all workers.
The persistent volume is mounted as: 

    /persistent

### SSH Key Volume
To enable password-free SSH among the pods for OpenMPI jobs, 
pods need to have a common ssh key pair. This key pair is distributed by 
a Kubernetes Secret object that must be created in advance of the job submission. 
This Secret object is mounted as a volume to the pods in the job. 
SSH start scripts use these keys to setup the password-free SSH access. 
SSH Key volume is mounted to pods as: 

    /ssh-key/openmpi 

### Working Directory
Twister2 jobs will run in the directory 

    /twister2 

All dependency library files are saved to the sub directory */twister2/lib*.
This directory is added to java CLASSPATH. All containers have their own /twister2 directory.
This directory is not shared among the containers in a pod. 
  
## Packaging and Running Twister2 Jobs in Kubernetes
We create a Docker container for the twister2 jobs. It will have all Twister2 library files 
and dependency library files. 
This container will be downloaded from a container store such as the Docker hub. 
Therefore, there is no need to transfer the Twister2 library packages to pods explicitly. 
However, we need to transfer the job package to the pods explicitly when submitting a job. 

### Twister2 Job Package
Twister2 job package will be a tar file. It will be generated for each job submission on the fly. 
It will contain:
* User job java files. The codes user wants to run on Twister2. 
* Configuration files for the job and the environment. 
* Serialization of the Job object. Job specific dynamic values.  

Twister2 job package will be generated on the submitting client machine, when submitting the job.
It needs to be transferred to each Kubernetes pod.

### Job Package Transfer Methods
We implemented two types of job package transfer to pods. 
* Job Package Transfer Through a Web Server
* Job Package Transfer Using kubectl file copy

### Job Package Transfer Through a Web Server
**Uploading**: Twister2 provides a number of uploaders. These uploaders can upload 
the job package to a server. We use any one of these uploaders to upload 
the job package to a web server directory. 

**Downloading**: We developed a downloader script that runs inside of pods. 
When a pod starts, the first container in the pod downloads the job package from the web server
using wget utility. It unpacks the job package to the shared memory directory. 
After unpacking it, it writes a flag file to let other containers to know that it has finished 
downloading and unpacking the job package.  

When containers others than the first container runs the downloader script. 
They just wait for the first container to download and unpack the job package. 
They periodically poll the existence of the flag file and sleep in between. 
When they see that the flag file is generated, they set the classpath and start the worker. 

The name of the downloader script is: *get_job_package.sh*

Disadvantage: This is a simple and effective method to transfer the job package to many pods. 
However, it requires the administrators to set up a web server. 
All users of Twister2 need to have write permissions to its directory to upload the job package.
This may be cumbersome particularly for fast installations and testing. 
The second approach does not require a web server to work. 

### Job Package Transfer Using kubectl file copy
Kubernetes kubectl command provides a file copy command: kubectl cp.
By using this command, one can transfer files from local machines to pods and vice versa.

We transfer the job package to pods by using this “kubectl cp” command. 
The client submitting the job needs to run on a machine where kubectl is configured to run 
with the Kubernetes master. We believe that should be a reasonable assumption. 

#### Starting Job Package Transfer
We transfer the job package in parallel to all pods in the job. 
However, first we need to know that the pods are ready to accept the file transfer. 
After sending StatefulSet create request to Kubernetes master, 
it takes some time to initialize the pods in the cluster. 
We monitor the status of all pods in the job to start the file transfer. 
Once the first container in a pod becomes “Started”, 
we start the job package transfer to that pod. 

Alternatively, we can start transferring the job package when the phase of the pod reaches 
to “Running” state. However, this takes longer than necessary. 
Because pod phase reaches to “Running” after all containers are started in a pod. 
When there are 10 containers in a pod, it takes around 7 seconds to start all containers. 
So the pod becomes Running after that much time. We decided to watch the events for a pod 
and get the container “Started” events instead. After the first container is started in a pod, 
we try to transfer the file. If it fails, we retry after waiting some milliseconds. 
Usually it succeeds in the first try but sometimes it may fail and succeed in the second try. 
Occasionally it may succeed in third or fourth tries. 

#### Parallel Transferring of the Job Package
Transferring the job package to a pod may take some time, 
since “kubectl cp” command runs as a separate process. 
It is not implemented in Kubernetes java client library. 
Therefore, it is much faster to transfer the job package to each pod in parallel 
from separate threads.
 
A separate thread (JobPackageTransferThread) is started to transfer the job package to each pod. 
It waits and checks the status of the assigned pod from PodWatcher list. 
When its pod becomes started, it tries to transfer the job package 
by starting a process to use “kubectl cp” command. 
Sometimes this command may fail. In that case, it retries to send the package. 
It retries based on the value of MAX_FILE_TRANSFER_TRY_COUNT. 
The value of this constant can be changed in the source code. 

#### Unpacking the Job Package
The job package is transferred to the shared directory in each pod. 
When there are multiple containers in a pod, only one of them needs to unpack it. 
Always the worker in the first container unpacks the job package. 
After unpacking, it writes a flag file to let other workers to know in the same pod. 
Other workers in the same pods watches this flag file and proceed 
after it is created by the first worker. 

#### Monitoring the status of Job Package Transfer
When the job package transfer starts, it takes some time. 
First workers in the pods need to wait for the the full job package to arrive before unpacking it. 
They checks the existence of the job package in the shared directory periodically. 
When they see that the job package file is created, they check the file size. 
When the the size is the same as the value specified in the environment variable, 
they assume that the job package transfer has completed. 
They unpack the job package and write the flag file to the shared directory.
Job submitting client sends the job package file size as an environment variable to all pods.

## Limitations and Future Works
**Dynamic Worker Additions/Removal**: In our current implementation, new workers can not be added
after a job is started or workers can not be removed from the job when it is running. 
Dynamic worker additions and removal can be implemented. 

**Worker Restarts After Failures**: We need to go over the worker implementations 
to make sure that the workers function properly after failures. 
Currently, this has not been carefully tested. 

## Kubernetes Objects for Twister2 Jobs
When creating Twister2 jobs on Kubernetes, we deploy many Kubernetes objects. 
A regular Twister2 job consists of the following Kubernetes objects:
* A StatefulSet object for Twister2 workers
* A Service object for the StatefulSet of workers
* A StatefulSet object for the Job Master
* A Service object for the StatefulSet of Job Master
* A PersistentVolumeClaim object for the persistent storage

The above objects are created when a job is submitted dynamically. 
We also create some long living objects on Kubernetes master. 
Those objects are created only once and they live on the cluster always. 
These long living objects are:
* A Secret object for password-free SSH access among pods for OpenMPI
* A Role and RoleBinding object for giving pods required privileges

### YAML Templates
Kubernetes objects are created either using YAML files or using a programming API. 
When using YAML files, kubectl command is used to send the requests to Kubernetes master.
We create the Twister2 dynamic objects using Kubernetes Java API. 
This is more convenient and easier for users. 
However, designing YAML files help understanding the objects involved in creating the jobs. 

We designed the YAML file templates for each dynamically created Kubernetes object. 

[**StatefulSet object for Twister2 workers**](yaml-templates/statefulset-for-workers.yaml): 
This is the most important object. It will create the pods for workers to run. 
It describes the features of the pods. Their computing resources, volume needs, labels, etc. 
 
**Service object for Twister2 workers**: 
Each StatefulSet objects requires to have an associated service object. 
We have two types of Service objects. One of them is used for the StatefulSet of workers. 
* [Headless Service for Twister2 workers](yaml-templates/service-headless.yaml):
A simple headless service. This does not export any services outside of the cluster. 

* [NodePort Service for Twister2 workers](yaml-templates/service-nodeport.yaml):
A simple NodePort service. It lets workers to get requests from entities outside of the cluster.
 
[**StatefulSet object for the Job Master**](yaml-templates/statefulset-for-job-master.yaml): 
This is the object that describes the pod of the Job Master. 
Since the Job Master computing needs may be different than worker computing needs,
we designed a separate StatefulSet object for the Job Master. 
 
[**Service object for the Job Master**](yaml-templates/service-job-master.yaml): 
This is simple headless Service object for the StatefulSet object of the Job Master. 

[**PersistentVolumeClaim object**](yaml-templates/persistent-volume-claim.yaml): 
This is the PersistentVolumeClaim object that will be used by all Twister2 pods in a job. 
Its capacity is the total persistent storage capacity of all workers and the Job Master. 

### Long Living Kubernetes Objects

[**Role and RoleBinding objects**](../../../installation/kubernetes/twister2-auth.yaml): When Twister2 is installed in a cluster,
a Role and RoleBinding object need to be created for the namespaces that will execute 
Twister2 jobs. This can be executed by the Kubernetes administrator once.
First, the namespace field in that file needs to be changed. Then, 
the following command needs to be executed:

    $kubectl create -f twister2-auth.yaml
  
[**Secret Object**](yaml-templates/secret.yaml): 
When using OpenMPI communications in Twister2, pods need to have password-free SSH access 
among them. This is accomplished by first generating an SSH key pair and 
deploying them as a Kubernetes Secret object on the cluster. 
Please check [the document](../../../installation/kubernetes/twister2-kubernetes-install.rst) for deploying the Secret object.
