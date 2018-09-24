ZooKeeper based Worker Discovery
================================

We designed a worker discovery, synchronization and ID assignment system
for multi-worker jobs in cluster environments that use a ZooKeeper server.

We developed the following class:

    edu.iu.dsc.tws.rsched.bootstrap.ZKWorkerController

It implements the interface:

    edu.iu.dsc.tws.common.discovery.IWorkerController

We provide the following services: 
* Assigning unique IDs to each worker from zero and increasing sequentially without any gaps.
* Getting the list of all joined workers in a job including the ones that have already left.
* Waiting for all workers to join the job.
* Waiting all workers on a barrier point

Above services are required by the implemented interface. 
In addition, ZKController class provides the following service: 
* Getting the list of all currently running workers in a job.

## Assumptions
Each Twister2 job has a unique name: There can not be more than one Twister2 job running 
in the cluster with the same name. When we submit a job, 
if there is already a running job with the same name, that submission must fail.

Each Twister2 job may have any number of workers.

When a Twister2 worker starts in a cluster, it knows its IP address and its port number. 
Each worker is assigned at least one port number.

## Main Idea
ZooKeeper keeps data as a tree of znodes. Similar to the file system in computers. 
Each znode is identified by its path from the root. Znodes can have children znodes. 
All znodes can also hold some data. 

We create a znode for each job. We use the job name as the znode name, since the job names are unique. 
The body of the job znode keeps the list of all joined workers in the job 
including the ones that have already left.
Each line of the job znode body has the data of a single worker. 
The format of the job znode body is as follows:

    ip,port,workerID;nodeIP,rackName,datacenterName
    ip,port,workerID;nodeIP,rackName,datacenterName
    ....
    ip,port,workerID;nodeIP,rackName,datacenterName

When a worker joins the job, it first gets its unique ID. 
Then, it updates the body of the job znode with its data. 
It adds its line to the end of the body.

Then, each worker creates a separate child znode under the job znode. 
The name of the child znode is composed of workerIP and port number: 

    workerIP:workerPort

Since workerIP and workerPort pair is unique in each job, this prevents any collusion. 
Each worker adds its data as the body data of its znode:

    ip,port,workerID;nodeIP,rackName,datacenterName

When a worker completes and leaves the job, its child znode is deleted. 
However, it does not delete its data from the body of the job znode. 
Therefore, children znodes provide data for the current list of workers in a job. 
The body of the job znode provides data for all joined workers 
including the ones that have already left.

The first worker to register with the ZooKeeper server creates the znode for that job. 

### Removing Worker Znodes from ZooKeeper Server
When a worker finishes the computation, its znode should be deleted from the ZooKeeper. 
Therefore, we create an ephemeral znode on the ZooKeeper server. 
When a worker closes its connection to the ZooKeeper server, 
its znode is deleted automatically. 

### Removing Job Znode from ZooKeeper Server
The job znode can not be ephemeral, since ephemeral znodes can not have children. 
Therefore, the last worker to finish computation needs to remove the job znode. 
When workers have finished computation, they check whether they are the last worker. 
If so, they remove the job znode. 

### Failing to Remove the Job Znode
When the last worker fails and can not properly complete the computation, 
it can not delete the job znode. Then, the job znode may live on the ZooKeeper server 
after the job has completed. 

In another scenario, when a worker fails, ZooKeeper server may take some time 
to determine that failure. Currently, it takes 30 seconds for the ZooKeeper server 
to determine a failed client. Therefore, failed worker znodes are deleted after 30 seconds. 
During this time, if the last worker completes and leaves the job, 
it thinks that it is not the last worker. So, it does not delete the job znode. 
The job znode may live on the ZooKeeper server after the job has finished. 

Yet in another case, if the last two workers leave almost at the same time 
with a few milliseconds apart, both think that they are not the last one to leave. 
Since, they have not received the worker leave updates yet. 
So, the job znode may not be deleted.

### What happens When a Job znode is not deleted
When a job znode is not deleted after the completion of a job, 
it can be deleted when a new job is submitted with the same name. 
Or when a terminate job command is executed for that job. 

## Assigning Unique Worker IDs
We use DistributedAtomicInteger class of Curator library to assign unique IDs to workers. 
This class provides a shared integer counter that is attached to a znode 
on the ZooKeeper server and shared by all workers in a job. 
When a worker joins the job, it increases its value by one and uses 
the previous value as its unique ID. Since the increment operation is atomic, 
no two workers can get the same ID. We assign this ID only after a successful 
increment of the shared variable. The counter starts from zero. 
So, the first worker gets the ID zero. 

### Getting Worker IDs after Failures
When a worker rejoins a job, it is assigned its previous ID. 
This prevents ID sequences to have gaps in them in the case of failures. 

When a worker joins a job, it first checks the body of the job znode. 
If there is an entry with this workers <IP>:<port> pair, 
it means that this worker is rejoining the job after a failure.
It uses the workerID from that line in the job znode body. 
It does not generate a new unique ID for itself. 

If there is no <IP>:<port> pair for this worker in the body of the job znode, 
then it generates a new ID and posts it to the job znode body. 

## Waiting Workers on a Barrier
We use DistributedBarrier class of Curator library to make workers wait on a barrier point. 
However, this class does not support the number of workers to wait. 
We need to watch the waiting workers and signal the barrier to release the workers,
when all workers arrived at the barrier. 

We use a DistributedAtomicInteger object from Curator library to count the number of waiting workers 
on the barrier point. Each worker increases the value of this counter by one, when they come to 
the barrier point. After increasing this distributed counter, they start to wait 
on the distributed barrier object. The last worker to arrive at the barrier point, 
does not wait. Instead, it tells the barrier object to release all waiting workers.
 
The last worker checks the value of the distributed counter, and if the counter value is a multiple of 
the numberOfWorkers in the job, it understands that it is the last worker.

Workers can wait multiple times on the barrier points during a job lifetime. 

## Trying to Create a Job When Another Running
When a Twister2 job is submitted by the client, 
submitting client first must check whether there is a znode created for that job name
on the ZooKeeper server. If there is a znode with the same job name, 
there are two possibilities:
* Another job with the same name may be running
* Previously submitted and completed job is not cleaned properly

If the job znode has some children, it can be assumed that 
there is a job already running on the cluster with the same name. 
Job submission must fail. The user can resubmit the job with another name, or 
can wait until the running job to complete.

If the job znode does not have any children, it means that 
a previously executed job is not cleaned properly from ZooKeeper server. 
Job submitting client can remove that job znode automatically and 
proceed with the job submission.

We provide a utility class to check whether there is a job znode on the ZooKeeper server
and delete the job related znodes if necessary:

    edu.iu.dsc.tws.rsched.bootstrap.ZKUtil

## Implementation Details:
We use Apache Curator software to connect and manage communication between workers 
and the ZooKeeper server.

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
attached to the same znode. Workers acquire the shared lock to update the job znode body.
 
Since more than one worker may update the body of the job znode concurrently, 
workers required to acquire this lock before updating the job znode body.  
They release the lock after they updated it. 

## Usage
When a worker starts, it first needs to create an instance of ZKWorkerController class and 
initialize it by calling its initialize method. Then, it can get its unique ID by calling
getWorkerNetworkInfo() method of ZKWorkerController object. 

It can call getWorkerList() method of ZKWorkerController object to get the list of currently
joined workers immediately. Or, if it needs the full list of workers in the job. Then,
it can call waitForAllWorkersToJoin(timeLimit) method of ZKWorkerController object. 
This method will wait until either getting the full list of workers in the job or 
the time limit has been reached. 

A sample usage can be found in the class:

    edu.iu.dsc.tws.examples.internal.bootstrap.ZKControllerExample.java

Its usage in the following class can also be examined for real usage:

    edu.iu.dsc.tws.rsched.schedulers.aurora.AuroraWorkerStarter

### Configuration Parameters
Following configuration parameters must be specified in the configuration files:

    List of ZooKeeper server IP:port value: twister2.zookeeper.server.addresses
    Example value: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"

Following configuration parameters have default values and their default 
values can be overridden in the configuration files:

Root znode name is by default: "/twister2" 
It can be changed with the configuration parameter: 

    twister2.zookeeper.root.node.path

Max wait time for all workers to join default value is 100 seconds. 
It can be changed by the following parameter: 

    twister2.zookeeper.max.wait.time.for.all.workers.to.join
