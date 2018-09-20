Twister2 OpenMPI Support in Kubernetes
======================================

We want the Twister2 workers to run OpenMPI jobs. OpenMPI jobs are different 
than regular jobs in a number of ways. OpenMPI requires that:
1. extra libraries are installed on the pods 
2. pods have password-free SSH access among them.
3. MPI master pod has a hostfile with the list of all pod addresses before starting the job
4. MPI master node starts the job by executing mpirun command. Then each worker 
started by OpenMPI framework.
5. OpenMPI framework assigns workers ranks (ids).

## Extra Libraries in Pods
We installed OpenMPI and OpenSSH libraries when we build the containers.

## Password-free SSH Among Pods
We start an OpenSSH server in each pod when we run OpenMPI enabled Twister2 jobs. 
We have an SSH key pair that are shared among all pods. This key pair is distributed to 
all pods by a Secret object in Kubernetes cluster. This Secret object is created once 
for a cluster and it is a long living object. It can be used by all users who share the same 
namespace. 

The Secret object is mounted to each pod as a volume on:

    /ssh-key/openmpi

SSH start scripts use the keys from this volume and set up the password-free SSH access
among the pods.  

## Hostfile Generation
When an OpenMPI enabled job is submitted, init_openmpi.sh script is executed in each pod.
This script first starts the OpenSSH server and sets up the password free access among the pods.
Next, this script first gets the job package as it is explained in job package upload section. 
Then, all pods except the first pod in the job starts sleeping. First pod of the job is the its name
has the suffix "-0". The first pod starts the following class:
 
    edu.iu.dsc.tws.rsched.schedulers.k8s.mpi.MPIMasterStarter

to generates the hostfile first. This class watches the pods in the job by listening to events 
thorough the Kubernetes master. When they become "Running", it gets their IP addresses and
saves them to "hostfile". It also retrieves the IP address of the Job Master. 

After generating the hostfile, this pod makes sure that all OpenSSH servers are running 
in every pod. It executes the script "check_pwd_free_ssh.sh".  
This scripts tries to connect to each pod in the job with ssh. If it can not succeed, 
it retries. The reason for this is that a pod may have started but OpenSSH server may have not
started on it yet. If we execute mpirun without waiting OpenSSH to be started on all pods, 
it fails. Since it can not connect to some pods with SSH. Therefore, before executing mpirun, 
we make sure that SSH servers are started and working properly on each pod. 

To check the ssh connection among two pods, we send a dummy ssh request. 
We ask for "pwd" with ssh. Other simple shell commands can also be sent. 

## Starting MPI Job
After generating the hostfile and making sure that password-free ssh is working from the first 
pod to all pods, MPIMasterStarter class executes "mpirun" as a separate process.
It tells how many workers to run on each pod. It also send the Job Master IP address to 
workers, so that they don't try to discover Job Master IP address again. 

MPIMasterStarted class asks mpirun to start the following worker starter class on all pods:

    edu.iu.dsc.tws.rsched.schedulers.k8s.mpi.MPIWorkerStarter

An instance of this class is started in each pod. This class in return will start the
Twister2 worker classes that is defined in configuration files. 

## Worker ID Assignment
In regular Twister2 jobs, we calculated the workerIDs based on their pod names and 
container names as it is explianed in [the document](twister2-on-kubernetes.md).    

When using OpenMPI, since openMPI is assigning unique IDs to each worker, 
we use the rank of OpenMPI as the workerID.

## Configuration Parameters
Since OpenMPI jobs are started differently than regular Twister2 jobs, 
a configuration parameter is used to tell whether a job will use OpenMPI. 

The parameter name is:

    kubernetes.workers.use.openmpi

The value of this parameter has to be "true" to enable OpenMPI. 
By default, the value of this parameter is false. 

A second parameter specifies the name of Kubernetes Secret object. 
Secret object can ne either created by the administrator when Twister2 is installed or
created by a user. However, its name must be specified as a parameter. Parameter name: 

    kubernetes.secret.name

