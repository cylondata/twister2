ZooKeeper Based Job Master Discovery
====================================

When a Twister2 job is submitted, a Job Master is created in addition to workers. 
Workers need to know the Job Master IP address and the port number to be able to connect to it. 
Some resource schedulers such as Kubernetes provides some means for workers to discover the Job Master. 
However, there might be resource schedulers that do not provide such discovery mechanisms such as Nomad scheduler.
In that case, ZooKeeper server can be used to discover the Job Master address.

## Main Idea
When a Twister2 job is submitted:
* one Job Master
* possibly many workers  

started in the cluster.

**When the Job Master starts:** 
* It registers its IP address and the port number with the ZooKeeper server.

**When the workers start:**
* They get the address of the Job Master from the ZooKeeper server. 

## Assumptions
We assume that:
* Each Twister2 job has a unique name. 
There can not be more than one Twister2 job running in the cluster with the same name simultaneously. 
When we submit a job, if there is already a running job with the same name, that job submission fails.
* Each Twister2 job is composed of one Job Master and possibly many workers.
* Job Master knows its own IP address and its port number when it starts.
* Both the Job Master and the workers know the IP address and the port number of the ZooKeeper server.

## Implementation
**We implemented two classes:**
* edu.iu.dsc.tws.rsched.bootstrap.ZKJobMasterRegistrar
* edu.iu.dsc.tws.rsched.bootstrap.ZKJobMasterFinder

ZKJobMasterRegistrar class is used to register the Job Master address with the ZooKeeper server.  
ZKJobMasterFinder class is used to get the Job Master address from the ZooKeeper server. 

**ZNode Creation**  
When the Job Master registers its address on the ZooKeeper server, 
an ephemeral znode is created. The name of this znode will be:  

    /twister2/<job-name>-<job-master>  

assuming the root znode is twister2.

Job Master IP address and the port number is put as the payload in this znode as a String in the form of:

    ip:port 

**ZNode Deletion**  
When the job completes, the ZKJobMasterRegistrar should delete the znode from the ZooKeeper server explicitly by calling its close method. 
If the job master is prematurely shut down, the znode will be deleted automatically, since the znode is ephemeral.
However, it takes 30 seconds for the ZooKeeper to delete ephemeral nodes in premature shut downs. 
If the user wants to submit another job during this time period with the same name, then the remaining znode 
from the previous job needs to be deleted first.

ZKJobMasterRegistrar class has two methods to check the existence of the znode for that job and delete it:
* sameZNodeExist() 
* deleteJobMasterZNode()  

These methods can be used to clear the previously remaining znodes. 
However, care needs to be taken, because another user may have been submitted a job with the same name. 
Before deleting the job master znode, the user needs to be sure that, that znode is its znode from previous job submission.  

**Discovering the Job Master Address**  
When the workers start, they query the job master znode. 
If the znode already created, they get the content and parse the ip:port pair. 
If the znode has not been created yet, we create a NodeCache object. 
It gets all changes to the Job master node in a local cache. 
We get the Job Master address from this cache when the znode is created on the server. 
NodeCache works event based. So, we avoid polling the ZooKeeper server continually.      

## Usage
**Registering the Job Master**  
Job Master is started by a separate program in each cluster.
For example, in Kubernetes it is started by the class:  

    edu.iu.dsc.tws.rsched.schedulers.k8s.master.JobMasterStarter

When this class starts the Job Master, it also needs to start the ZKJobMasterRegistrar. 
When the ZKJobMasterRegistrar is constructed, its initialize method needs to be called to register. 
It returns a boolean value to show the result of the registration. 

When the job completes, it needs to call the close method of the ZKJobMasterRegistrar class 
to delete the job znode and close the connection to the ZooKeeper server. 

**Getting the Job Master Address**  
Similar to Job Master, Twister2 workers are also started by a separate program in each cluster. 
For example, in Kubernetes clusters, they are started by:

    edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerStarter  

Before starting the worker, this program should start ZKJobMasterFinder. 
After initializing, it can get the Job Master address by calling the method: 

    getJobMasterIPandPort().

If this method returns null, it means that the Job Master has not registered yet. 
In that case, it can call the method 

    waitAndGetJobMasterIPandPort(long timeLimit)

This method will wait for the Job Master znode to be registered until the timeLimit has been reached. 

**Example Code**  
A sample usage of ZKJobMasterRegistrar is provided in the example class:

    edu.iu.dsc.tws.examples.internal.bootstrap.ZKJobMasterRegistrarExample.java

Its corresponding sample usage of ZKJobMasterFinder is provided in the example class:

    edu.iu.dsc.tws.examples.internal.bootstrap.ZKJobMasterFinderExample.java
