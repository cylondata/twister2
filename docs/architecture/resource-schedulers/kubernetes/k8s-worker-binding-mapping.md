Twister2 Worker Binding and Mapping in Kubernetes
=================================================

Worker binding is the process of binding workers to CPU cores. 
Bounded workers are prevented to be moved from one core to the other during their lifetimes. 
They finish the execution in the core that they are started.
 
Worker mapping is the process of assigning workers to nodes around the cluster. 
Some jobs may need to run in some specific machines. Some machines may have special hardware 
such as GPUs or fast SSD disks. Or some nodes may have special software. 
Therefore, some jobs may need to run on some specific machines. 
On the contrary, some jobs may want to NOT run on some machines.
They may run in any machine but some of them. Worker mapping handles 
the distribution of workers to nodes in the cluster.

## Worker Binding
Kubernetes allows binding of pods to CPU cores. It is called static binding. 
When a pod is started with static binding, that pod has exclusive 
ownership of the assigned core(s). Therefore, this pod is not moved to 
any other core during its lifetime. 

When CPUs are requested for workers with static binding, CPUs per worker 
has to be an integer. When a pod is statically bounded to a core, 
it can not have fractional ownership of that core. 
It has to have the total access to the core. 
However, one worker (container) can have more than one cores, but not fractional cores.
Kubernetes use [Linux CPUSETS](https://www.kernel.org/doc/Documentation/cgroup-v1/cpusets.txt) 
to implement exclusive binding of pods to cores.

### Configuration Parameters
A boolean configuration parameter is added to the configuration files. 
If its value is set to “true”, static binding is performed. 
Otherwise, workers are not bounded to cores. They may be moved during their lifetimes 
to other cores. The configuration parameter is: 

  	kubernetes.bind.worker.to.cpu

### Binding All Workers
When worker binding is requested, all workers in that job is bound to their cores. 
There is no binding for only some workers in a Twister2 job. 
Either all workers are bounded or none.

### Binding Inefficiency
Worker binding should be used with caution. Since only the assigned worker can use the cores, 
those cores may go idle sometimes and no other pods in the system can use them during 
their lifetime. Unnecessary use of worker binding may result in less than 
optimal utilization of computing resources. 

## Worker to Nodes Mapping
Kubernetes provides some mechanisms to map pods to nodes in a cluster. 
When submitting a job, users can request that worker pods to be started 
in nodes with some labels. Or they can request that pods not to be started 
in some nodes with some labels. In any case, pod assignment to nodes are handled 
by using labels. 

In the first step, node labels are created for cluster machines. 
In the second step, users request their worker pods to be started based on those labels. 
Node labeling is performed by using kubectl command line tool. 
We assume that node labeling is performed prior to creating Twister2 jobs. 
The format of the label creation command is as follows:

    >kubectl label node <node-name> <label-key>=<label-value>

**Predefined Labels**: There are also some predefined labels in Kubernetes for each node. 
Users can request mapping based on these labels also. Some of these labels are as follows: 
* kubernetes.io/hostname, 
* beta.kubernetes.io/os, 
* beta.kubernetes.io/arch. 

The primary reason for requesting workers to be located in some nodes is that 
not all machine nodes in a cluster are the same. Some nodes might have different hardware 
such as faster disks, faster CPUs, or extra hardware such as GPUs, 
they might have extra installed software such as databases. 
These machines are first labeled for these features and then users submit 
the jobs for their workers to be scheduled on the requested nodes. 
 
### Worker Mapping Implementation
When requesting pod placement to nodes, users provide three types of data:
* label-key
* operator
* label-values

There are currently six operators: 
* In, NotIn, Exists, DoesNotExist, Gt, Lt.

Two of the operators check for equality: 
* In and NotIn

Two of the operators check for the existence of the given labels in nodes 
(They don’t check the values of the keys):
* Exists, DoesNotExist
 
Two of the operators check for numerical comparison:
* Gt (greater than) and Lt (less than)

### Configuration Parameters 
In our Twister2 implementation, we allow users to specify these three parameters 
in configuration files. We have 4 configuration parameters. 
First one shows whether the user wants worker to nodes mapping. 
The other three determines the mapping parameters:

    kubernetes.worker.to.node.mapping: true or false
    kubernetes.worker.mapping.key: “key”
    kubernetes.worker.mapping.operator: “op”
    kubernetes.worker.mapping.values: [‘list’ ‘of’ ‘values’]

### Example 1: 
Let’s assume that we have a cluster with 10 nodes. 
4 of them have GPUs and they are labeled as “extra-hardware=gpu”. 
We want to start our workers in gpu installed nodes. 
Users specify the worker mapping parameters as:

    kubernetes.worker.to.node.mapping: true
    kubernetes.worker.mapping.key:  “extra-hardware”
    kubernetes.worker.mapping.operator:  “In”
    kubernetes.worker.mapping.values: [‘gpu’]

This makes sure that the workers are started only in one of those 4 gpu installed nodes. 
Since values can have more than one value, it is given as a list of strings.
 
As another example, the user might want to start the workers in non-gpu installed nodes. 
In that case, configuration values will be: 

    kubernetes.worker.to.node.mapping: true
    kubernetes.worker.mapping.key:  “extra-hardware”
    kubernetes.worker.mapping.operator: “NotIn”
    kubernetes.worker.mapping.values: [‘gpu’]

### Example 2: 
We can also use default keys when requesting worker mapping to nodes. 
Let’s use node name labels. Let’s assume that the nodes have names:  
node01, node02, node03, … , node9. 

We can request that the workers to be placed in one of three nodes such as 
node03, node04, node05. In this case, the configuration settings would be: 

    kubernetes.worker.to.node.mapping: true
    kubernetes.worker.mapping.key:  "kubernetes.io/hostname"
    kubernetes.worker.mapping.operator: “In”
    kubernetes.worker.mapping.values: [‘node03’, ‘node04’, ‘node05’]
    
Or we can request that the workers not to be placed in one of those nodes:

    kubernetes.worker.to.node.mapping: true
    kubernetes.worker.mapping.key:  "kubernetes.io/hostname"
    kubernetes.worker.mapping.operator: “NotIn”
    kubernetes.worker.mapping.values: [‘node03’, ‘node04’, ‘node05’]

### Limitations
Currently users can set only one set of configuration parameters when submitting a job. 
The user can not requests worker placements based on more than one label key. 
However, Kubernetes supports that feature. This feature may be added to Twister2.

## Uniform Worker Mapping
In addition to node affinity, Kubernetes also provides pod affinity mechanism. 
With this method, pods can be deployed with respect to the other pods running in the cluster. 
Instead of directly supporting this service, we implemented two specific cases of 
this service that distributes pods uniformly.

**all-same-node**: When uniform worker mapping is requested with “all-same-node” value, 
all pods in that job are placed into the same node. This does not say anything about 
the node to be deployed. All it requires that all pods belonging to 
this job will be started on the same node. It can be any node in the cluster 
or any node that is determined by node mapping settings. 

**all-separate-nodes**: When uniform worker mapping is requested with “all-separate-nodes” 
value, all pods in that job are placed into separate nodes. 
Only one pod is placed into one node. If there are not enough nodes in the cluster, 
the job can not be started. However, this does not guarantee that only 
one worker will be running on each node, since there might be more than one worker in each pod. 
Pod placement applies to only pods but not directly to workers. 
If each pod runs only a single worker, then worker placement becomes equivalent 
to pod placement.

### Configuration Parameter
Configuration parameter for uniform worker mapping is: 

    kubernetes.worker.mapping.uniform

Its value can be: "all-same-node", "all-separate-nodes", "none"

By default, its value is "none".

## Using Worker Mapping and Binding Settings Together
Worker mapping and uniform mapping can be used together. 
However, they should not conflict each other. They should complement each other. 

For example, in the above cluster with 10 nodes and 4 of them having GPUs, 
one can specify both worker mapping and uniform mapping together. 
Let’s say, we want to run one pod on each GPU node. 
We can specify that workers to be started in GPU installed nodes only and 
they should be uniformly distributed with all-separate-nodes option. 
In this case, there can be at most 4 pods running in the job though. 
If we want to start more than 4 pods with the same settings, 
then the job can not be started. 

Worker mapping and worker binding can be used together also. They do not conflict each other. 
However, there must be enough resources for worker binding. 

## Future Works
Pod affinity support can be improved. More cases can be added. 
For example, the pods of a job can be requested to run only with some other pods. 
Let’s assume that there are four pods running in the cluster for a database server, 
we can start one pod next to each database server. Or, we can say that our pods 
should not run in any node that is running this database pods. 