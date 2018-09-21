Worker Discovery and Synchronization in Twister2 Jobs
=======================================================

We developed an interface: IWorkerController and provided its implementations by 
using various cluster services. This interface provides the following services:
* Unique ID assignment to workers
* Discovery of Worker addresses in a job
* Synchronizing workers using a barrier

**Unique ID assignment to workers**: We require that each worker in Twister2 jobs has 
a unique sequential ID starting from 0. When N workers are started in a Twister2 job, 
the workers will have the unique IDs in the range of 0 to (N-1).

**Discovery of Worker addresses in a job**:
We assume that each worker in a Twister2 job has a unique IP address and port number pair.
More than one worker may run on the same IP address, but they must have different port numbers. 
So, each Twister2 worker in a job must have a unique ID and a unique IP:port pair. 

When a Twister2 job is scheduled in a cluster, usually multiple Twister2 workers are started 
in that cluster. All workers need to know the address of other workers in the job 
to be able to communicate with them. 

The submitting client does not know where the workers will be started on the cluster, 
when it submits the job. Therefore, it can not provide this information to the workers when they start.
Cluster resource schedulers start the workers in the nodes of the cluster. 

**Node Location Information**:
Twister2 can use worker locations when scheduling tasks. 
Workers may run in virtual machines such as pods in Kubernetes. 
So, worker IP addresses can be different from the IP address of the physical node it is running on. 
We provide a NodeInfo object for workers. 
It shows the physical node IP address for that worker. 
The rack name of the node it is running on. 
And the datacenter name where this node is running. 
Rack and datacenter names might not be available in all clusters.

**Synchronizing workers using a barrier**:
All workers in a Twister2 job may need to synchronize on a barrier point. 
The workers that arrives earlier to the barrier point wait others to arrive.
When the last worker arrives to the barrier point, they are all released. 
 
## IWorkerController Interface
We designed an interface to be implemented by worker controllers and 
to be used by workers to discover other workers in a Twister job. 

The interface is: 
* [edu.iu.dsc.tws.common.discovery.IWorkerController](../../../../twister2/common/src/java/edu/iu/dsc/tws/common/discovery/IWorkerController.java)

## IWorkerController Implementations
We developed worker controllers implementing IWorkerController interface 
using various cluster services. 

### ZooKeeper Based Worker Controller
We implemented a worker controller using a ZooKeeper server. 
ZooKeeper server runs in many clusters. This worker controller can be used in those clusters. 
The worker controller class is: 
* [edu.iu.dsc.tws.rsched.bootstrap.ZKWorkerController](../../../../twister2/resource-scheduler/src/java/edu/iu/dsc/tws/rsched/bootstrap/ZKWorkerController.java)

Details of the implementation is provided in [the document](zk-based-worker-discovery.md). 

### Job Master Based Worker Controller
Twister2 runs a Job Master in Twister2 jobs. 
We also provide a Job Master based worker controller implementation.
The worker controller class is: 
* [edu.iu.dsc.tws.master.client.JMWorkerController](../../../../twister2/master/src/java/edu/iu/dsc/tws/master/client/JMWorkerController.java)

Details of the implementation is provided in [the document](../job-master/job-master.rst). 

### Kubernetes Master Based Worker Controller
We developed a worker discoverer that uses Kubernetes master 
to discover other workers in a Twister2 job.  
The worker discoverer class is: 
* [edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerController](../../../../twister2/resource-scheduler/src/java/edu/iu/dsc/tws/rsched/schedulers/k8s/worker/K8sWorkerController.java)

Details of the implementation is provided in [the document](../resource-schedulers/kubernetes/k8s-based-worker-discovery.rst). 

