# Worker Discovery and Synchronization in Twister2 Jobs
Ahmet Uyar

We developed an interface: IWorkerController and provided its implementations by 
using various cluster services. This interface provides the following services:
* Unique ID assignment to workers
* Discovery of Worker addresses in a job
* Synchronizing workers using a barrier

We require that each worker in Twister2 jobs has a unique sequential ID starting from 0. 
When N workers are started in a Twister2 job, the workers in that job will have the unique IDs 
in the range of 0 to (N-1).

We assume that each worker in a Twister2 job has a unique IP address and port number pair.
More than one worker may run on the same IP address, but they must have different port numbers. 
So, each Twister2 worker in a job must have a unique ID and a unique IP:port pair. 

When a Twister2 job is scheduled in a cluster, usually multiple Twister2 workers are started 
in that cluster. All workers need to know the address of other workers in the job 
to be able to communicate with them. 

The submitting client does not know where the workers will be started on the cluster, 
when it submits the job. Therefore, it can not provide the addresses of the workers when they start.
Cluster resource schedulers start the workers in the nodes of the cluster. 
When a worker starts, it needs to discover the IP addresses and port numbers of 
the Twister2 workers in that job.

All workers in a Twister2 job may need to synchronize on a barrier point. 
The workers that arrives earlier to the barrier point wait others to come.
When the last worker arrives to the barrier point, they are all released. 
 
## IWorkerController Interface
We designed an interface to be implemented by worker controllers and 
to be used by workers to discover other workers in a Twister job. 

The interface is: 
* [edu.iu.dsc.tws.common.discovery.IWorkerController](../../../twister2/common/src/java/edu/iu/dsc/tws/common/discovery/IWorkerController.java)

## IWorkerController Implementations
We developed worker controllers implementing IWorkerController interface 
using various cluster services. 

### ZooKeeper Based Worker Controller
We implemented a worker controller using a ZooKeeper server. 
ZooKeeper server runs in many clusters. This worker controller can be used in those clusters. 
Details of the implementation is provided in [the document](../zookeeper/ZKBasedWorkerDiscovery.md). 

### Job Master Based Worker Controller
Twister2 runs a Job Master in Twister2 jobs. 
We also provide a Job Master based worker controller.
The worker controller class is: 
* [edu.iu.dsc.tws.master.client.WorkerController](../../../twister2/master/src/java/edu/iu/dsc/tws/master/client/WorkerController.java)

Details of the implementation is provided in [the document](../job-master/JobMaster.md). 

### Kubernetes Master Based Worker Controller
We developed a worker discoverer that uses Kubernetes master 
to discover other workers in a Twister2 job.  
The worker discoverer class is: 
* [edu.iu.dsc.tws.rsched.schedulers.k8s.worker.WorkerController](../../../twister2/resource-scheduler/src/java/edu/iu/dsc/tws/rsched/schedulers/k8s/worker/WorkerController.java)

Details of the implementation is provided in [the document](../kubernetes). 

