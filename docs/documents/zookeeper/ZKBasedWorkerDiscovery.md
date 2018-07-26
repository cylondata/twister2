# ZooKeeper based Worker Discovery
Ahmet Uyar

We designed a worker discovery and ID assignment system for multi-worker jobs 
in cluster environments that use a ZooKeeper server.

We provide the following services: 
* Assigning unique IDs to each worker in a job starting from zero and 
increasing sequentially without any gaps.
* Getting the list of all currently running workers in a job with their communication information. 
* Getting the list of all joined running workers in a job including the ones that have already left.
* Waiting for all workers to join the job.  

## Assumptions
Each Twister2 job has a unique name: There can not be more than one Twister2 job running in the cluster with the same name. When we submit a job, if there is already a running job with the same name, that submission fails.

Each Twister2 job may have any number of workers.

## Trying to Create a Job When Another Running
When a job is submitted by the client, we first check whether there is a znode crated for that job 
on ZooKeeper server. If there is a znode with the same jobName, there are two possibilities:
* A job with same name may be runing
* Previously submitted and completed job is not cleaned properly

If the job znode has some children, we assume that there is a job already running on the cluster 
with same name. We print an error message and halt job submission. 
The user first needs to execute job terminate and then can resubmit the job. 
Or they can wait for the job to finish normally. Or they can submit the job with a different name. 

If the job znode does not have any children, it means that 
a previously executed job is not cleaned properly from ZooKeeper server. 
We remove that job znode automatically and proceed with the job submission. 

## Workers
Twister2 Workers are assigned to run as containers in clusters by container management systems 
such as Kubernetes and Mesos. Therefore, they can be initiated in any node around the cluster. 

When a worker starts in a container, it also gets the IP address of the container/pod it is running in 
and one port number to use to communicate with others. Therefore, each worker knows its own IP address 
and will have at least one port number to use when it is started. 

When a worker wants to communicate with other workers, it needs to know the IP addresses and port numbers of those workers. 
So all workers should know the communication information of all other workers in a job. 
### Worker IDs
Each worker in a job is assigned a unique ID. Worker IDs start from zero and increases sequentially. 
Workers get IDs in the order they created ZooKeeper znodes for themselves. 

## Using ZooKeeper for Worker Discovery and Unique ID Assignment
We use ZooKeeper server for workers to discover each other and get unique IDs for themselves. 

ZooKeeper keeps data as a tree of znodes. Similar to the file system in computers. 
Each znode is identified by its path from the root. Znodes can have children znodes. 
All znodes can also hold some data. 

We create a znode for each job. Then, each worker creates a child znode under this znode. 
Workers provide all the necessary information about themselves in their znodes. 
By monitoring the list of children znodes, all workers get the list of all other workers. 

The first worker to register with ZooKeeper server will create a Znode for that job. 
Then all workers create a child znode for themselves in that job znode. 

### Worker Names
When creating child znodes on the job znode, each worker needs to have a unique name. 
We use the <IP-Address>:<PortNumber> pair as the unique worker names. 

### Removing Worker Znodes from ZooKeeper Server
When a worker finishes the computation, its znode should be deleted from the ZooKeeper. 
Therefore, we create an ephemeral znode on the ZooKeeper server. 
When a worker closes its connection to the ZooKeeper server, 
its znode is deleted automatically. 

### Removing Job Znode from ZooKeeper Server
The job znode can not be ephemeral, since ephemeral znodes can not have children. 
Therefore the last worker to finish computation needs to remove the job znode. 
When workers have finished computation, they check whether they are the last worker, 
if so, they remove the job znode. 

### Failing to Remove the Job Znode
When the last worker fails and can not properly complete the computation, 
it can not delete the job znode. Then, the job znode may live on the ZooKeeper server 
after the job has completed. 

In another scenario, when a worker fails, ZooKeeper server may take some time 
to determine that failure. Currently it takes around 30 seconds for ZooKeeper server 
to determine a failed client. Therefore, it is currently deleting failed worker znodes 
after 30 seconds. During this time, if the last worker completes and leaves the job, 
it thinks that it is not the last worker, so it does not delete the job znode. 
So, the job znode may not be deleted. 

### What happens When a Job znode is not deleted
When a job znode is not deleted after the completion of a job, 
it can be deleted when a new job is submitted with same name. 
Or when a terminate job command is executed for that job. 

### Getting Unique Worker IDs
We use DistributedAtomicInteger class of Curator library to assign unique IDs to workers. 
This class provides a shared integer counter that is attached to a znode 
on the ZooKeeper server and shared by all workers in a job. 
When a worker joins the job, it increases its value by one and uses 
the previous value as its unique ID. Since the increment operation is atomic, 
no two workers can get the same ID. We assign this ID only after a successful 
increment of the shared variable. The counter starts from zero. 
So the first worker gets the ID zero. 

### Getting Worker IDs after Failures
When a worker rejoins a job, it is assigned its previous ID. 
This prevents ID sequences to have gaps in them in the case of failures. 

When a worker joins a job, it first checks whether there is an ID generated for itself. 
It checks the data of the job znode. All worker names and IDs are saved in the job znode. 
If there is an ID for itself, it means that it is rejoining. 
Therefore, it does not generate a unique ID. It uses the ID from the previous join. 
If there is no ID, it generates a new ID and posts it to the job znode. 

We use a distributed lock mechanism to update the data of the job znode. 
Since more than one worker my update concurrently, 
care needs to be taken to properly update. 
When a worker wants to update the job znode data, 
it first acquires the shared lock and updates the data. Then it releases the lock. 

## Implementation Details:
We use Apache Curator software to connect and manage communication between workers 
and ZooKeeper servers.

### Children Cache
Curator library implements a client side cache of a znode children:  

    org.apache.curator.framework.recipes.cache.PathChildrenCache

This cached children of a znode keeps an up-to date copy of the znode children 
in the client machine. In our case, each worker registers with the job znode 
and keeps a local copy of other worker znodes in the local. 
Therefore each worker keeps an up-to date list of all other workers in the job.

### Distributed Atomic Counter
Curator library implements a distributed atomic counter:  

    org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger

It provides a shared counter that is attached to a znode. We create a znode 
for this counter with “-dai” postfix: <jobName>-dai

### Distributed Lock
Curator library provides a distributed lock class:  

    org.apache.curator.framework.recipes.locks.InterProcessMutex

The lock is attached to a znode on the server. No two clients can acquire a lock 
attached to the same znode. Workers acquires the shared lock to update job node data. 

## Usage
When a worker starts, it first needs to create an instance of ZKDiscoverer class and 
initialize it by calling its initialize method. Then, it can get its unique ID by calling
getWorkerNetworkInfo() method of ZKDiscoverer object. 

It can call getWorkerList() method of ZKDiscoverer object to get the list of currently
joined workers immediately. Or, if it needs the full list of workers in the job. Then,
it can call waitForAllWorkersToJoin(timeLimit) method of ZKDiscoverer object. 
This method will wait until either getting the full list of workers in the job or 
the time limit has been reached. 

A sample usage can be found in the class:

    edu.iu.dsc.tws.examples.ZKDiscovererExample.java

Its usage in the following class can also be examined for real usage:

    edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraWorkerStarter

### Configuration Parameters
Following configuration parameters must be specified in the configuration files:

    ZooKeeper server IP: twister2.zookeeper.server.ip
    ZooKeeper server port number: twister2.zookeeper.server.port

Following configuration parameters have default values and their default 
values can be overridden in the configuration files:

Twister2 root node name is by default: "/twister2" 
It can be changed with the configuration parameter: 

    twister2.zookeeper.root.node.path

Max wait time for all workers to join default value is 100 seconds. 
It can be changed by the following parameter: 

    twister2.zookeeper.max.wait.time.for.all.workers.to.join
