# Worker Discovery in Twister2 Jobs
Ahmet Uyar

When a job is scheduled in a cluster, usually multiple Twister2 workers are started in that cluster.
All workers need to know the address of all other workers in the job to be able to communicate with them. 

The submitting client does not know where the workers will be started, when it submits the job. 
Therefore, it does not know the addresses of the workers to be started and 
it can not provide the addresses of the workers to the workers to be started in the job. 
Cluster resource schedulers start the workers in the nodes of the cluster. 
When a worker starts, it needs to discover the IP addresses and port numbers of 
the Twister2 workers in that job.

We also require that each worker in Twister2 jobs has a unique sequential ID starting from 0. 
When N workers are started in a Twister2 job, the workers in that job will have unique IDs in the range
of 0 to (N-1).

We also assume that each worker in a Twister2 job has a unique IP address and port number pair.
More than one worker may run on the same IP address,
but they must have different port numbers in that case. 

So, each Twister2 worker in a job must have a unique ID and a unique IP:port pair. 
 
## IWorkerDiscoverer Interface
We designed an interface to be implemented by worker discoverers and 
to be used by workers to discover other workers in a Twister job. 

The interface is: 
* [edu.iu.dsc.tws.common.discovery.IWorkerDiscoverer](../../../twister2/common/src/java/edu/iu/dsc/tws/common/discovery/IWorkerDiscoverer.java)

## IWorkerDiscoverer Implementations
We developed worker discoverers implementing IWorkerDiscoverer interface 
using various cluster services. 

### ZooKeeper Based Worker Discoverer
We implemented a worker discoverer using a ZooKeeper server. 
ZooKeeper server runs in many clusters. This worker discoverer can be used in those clusters. 
Details of the implementation is provided in [the document](../zookeeper/ZKBasedWorkerDiscovery.md). 

### Job Master Based Worker Discoverer
Twister2 runs a Job Master in Twister2 jobs. 
We also provide a Job Master based worker discoverer.
The worker discoverer class is: 
* [edu.iu.dsc.tws.master.client.WorkerDiscoverer](../../../twister2/master/src/java/edu/iu/dsc/tws/master/client/WorkerDiscoverer.java)

Details of the implementation is provided in [the document](../job-master/JobMaster.md). 

### Kubernetes Master Based Worker Discoverer
We developed a worker discoverer that uses Kubernetes master 
to discover other workers in a Twister2 job.  
The worker discoverer class is: 
* [edu.iu.dsc.tws.rsched.schedulers.k8s.worker.WorkerDiscoverer](../../../twister2/resource-scheduler/src/java/edu/iu/dsc/tws/rsched/schedulers/k8s/worker/WorkerDiscoverer.java)

Details of the implementation is provided in [the document](../kubernetes). 

