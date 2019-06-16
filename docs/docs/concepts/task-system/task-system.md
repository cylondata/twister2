---
id: concept_task
title: Task System
sidebar_label: Task System
---

The task layer provides a higher-level abstraction on top of the communication layer to hide the details of execution 
and communication from the user, while still delegating data management to the user. At this layer, computations are modeled as task graphs which
can be created either statically as a complete graph or dynamically as the application progresses. The task system comprises
of task graph, execution graph, and task scheduling process.

## Task Graph

The task graph is the preferred choice for the processing of large-scale data. It simplifies the 
process of task parallelism and has the ability to dynamically determine the dependency between 
those tasks. The nodes in the task graph consist of task vertices and edges in which task vertices 
represent the computational units of an application and edges represent the communication edges 
between those computational units. In other words, it describes the details about how the data is 
consumed between those units. Each node in the task graph holds the information about the input and 
its output. The task graph is converted into an execution graph once the actual execution takes place.

### Task Graph in Twister2

The task layer provides a higher-level abstraction on top of the communication layer to hide the 
underlying details of the execution and communication from the user. Computations are modeled as 
task graphs in the task layer which could be created either statically or dynamically. A node in 
the task graph represents a task whereas an edge represents the communication link between the vertices.
Each node in the task graph holds the information about the input and its output. A task could be 
long-running (streaming graph) or short-running (dataflow graph without loops) depending on the type 
of application. A task graph 'TG' generally consists of set of Task Vertices'TV' and Task Edges \(TE\) which is 
mathematically denoted as Task Graph

```text
(TG) -> (TV, TE)
```
### Static and Dynamic Task Graphs 

The task graphs can be defined in two ways namely static and dynamic task graph. 
 * Static task graph - the structure of the complete task graph is known at compile time.
 * Dynamic task graph - the structure of the task graph does not know at compile time and the program 
   dynamically define the structure of the task graph during run time.
 
The following three essential points should be considered while creating and scheduling the task 
instances of the task graph.

1. Task Decomposition - Identify independent tasks which can execute concurrently
2. Group tasks - Group the tasks based on the dependency of other tasks.
3. Order tasks - Order the tasks which will satisfy the constraints of other tasks.

\(Reference: Patterns for Parallel Programming, Chapter 3 \(2\) & 
[https://patterns.eecs.berkeley.edu/?page\_id=609](https://patterns.eecs.berkeley.edu/?page_id=609)\)

### Directed Task Graph and Undirected Task Graph

There are two types of task graphs namely directed task graph and undirected task graph. In directed
task graph, the edges in the task graph that connects the task vertexes have a direction as shown 
in Fig.1 whereas in undirected task graph, the edges in the task graph that connects the task 
vertexes have no direction as shown in Fig 2. The present task system supports only directed dataflow 
task graph.

![Directed Graph](assets/directed.png)  ![UnDirected Graph](assets/undirected.png)

### Streaming Task Graph

Stream refers the process of handling unbounded sequence of data units. The streaming application 
that can continuosly consumes input stream units and produces the output stream units. The streaming
task graph is mainly responsible for building and executing the streaming applications.

### Batch Task Graph

Batch processing refers the process of handling bounded sequence of data units. Batch applications 
mainly consumes bounded data units and produces the data units. The batch task graph is mainly 
responsible for building and executing the batch applications.

### Task Graph in Twister2

* The task graph system in Twister2 is mainly aimed to support the directed dataflow task graph 
  which consists of task vertices and task edges. 
   * The task vertices represent the source and target task vertex 
   * The task edge represent the edges to connect the task vertices
   
* The task graph in Twister2  
  * supports iterative data processing - For example, in K-Means clustering algorithm, at the end of 
    every iteration, data points and centroids are stored in the DataSet which will be used for the 
    next iteration 
  * It doesn’t allow loops or self-loops or cycles
    
* It describes the details about how the data is consumed between the task vertices.  
  * Source Task - It extends the BaseSource and implements the Receptor interface which is given below.
  * Compute Task - It implements the IFunction interface which is given below.
  * Sink Task - It extends the BaseSink and implements the Collector interface. 


## Task Scheduling

Task Scheduling is the process of scheduling the tasks into the cluster resources in a manner that 
minimizes the task completion time and utilizes the resources effectively. The other main functional 
requirements of task scheduling are scalability, dynamism, time and cost efficiency, handling different 
types of processing models, data, and jobs, etc. The main objective of the task scheduling is scheduling 
the task instances in an appropriate order to suitable/appropriate cluster nodes. The task scheduling 
algorithms are broadly classified into two types namely static task scheduling algorithms and dynamic 
task scheduling algorithms. The task scheduler in twister2 is designed in a way such that it is able 
to handle both streaming and batch tasks. It supports the following task scheduling algorithms namely 
RoundRobin, FirstFit, and DataLocality.

**RoundRobin Task Scheduling & DataLocality Aware Task Scheduling**

* Homogeneous Containers -&gt; Homogeneous Task Instances

**FirstFit Task Scheduling**

* Heterogeneous Containers -&gt; Heterogeneous Task Instances


### Implementation

The task scheduler has the main class named TaskScheduler which implements the base interface ITaskScheduler 
that consists of two main methods namely initialize and schedule methods to initialize the taskgraph 
configuration and schedule the taskgraph based on the workers information available in the resource plan.

```text
edu.iu.dsc.tws.tsched.spi.taskschedule.ITaskScheduler
```

has the following methods namely

```text
initialize (Config config)

schedule(DataflowTaskGraph graph, WorkerPlan plan)
```

The TaskScheduler is responsible for invoking the respective schedulers based on the type of task 
and the scheduling mode. If it is a streaming task, it will invoke the scheduleStreamingTask method 
whereas if it is a batch task it will invoke the scheduleBatchTask method.

```text
edu.iu.dsc.tws.tsched.taskscheduler.TaskScheduler
```

has the following methods such as

```text
scheduleStreamingTask()

scheduleBatchTask()

generateTaskSchedulePlan(String classname)
```

The scheduleStreamingTask and scheduleBatchTask call the generateTaskSchedulePlan with the scheduling 
class name as an argument which is specified in the task.yaml. The generateTaskSchedulePlan dynamically 
load the respective task schedulers \(roundrobin, firstfit, or datalocalityaware\) and access the 
initialize and schedule methods in the task schedulers to generate the task schedule plan.

### Proto file

The task scheduler has the protobuf file named taskscheduleplan.proto in the proto directory. It 
considers both the soft (CPU, disk) and hard (RAM) constraints which generates the task schedule plan 
in the format of Google Protobuf object. The proto file consists of the following details as follows. 
The resource object represents the values of cpu, memory, and disk of the resources. The task instance 
plan holds the task id, task name, task index, and container object. The container plan consists of 
container id, task instance plan, required and scheduled resource of the container. The task schedule 
plan holds the jobid or the taskgraph id and the container plan. The task schedule plan list is mainly 
responsible for holding the taskschedule of the batch tasks.

\`\`bash message Resource { double availableCPU = 1; double availableMemory = 2; double availableDisk = 3; }

```text
message TaskInstancePlan {
   int32 taskid = 1;
   string taskname = 2;
   int32 taskindex = 3;
   Resource resource = 4;
}

message ContainerPlan {
   int32 containerid = 1;
   repeated TaskInstancePlan taskinstanceplan = 2;
   Resource requiredresource = 3;
   Resource scheduledresource = 4;
}

message TaskSchedulePlan {
   int32 jobid = 1;
   repeated ContainerPlan containerplan = 2;
}

message TaskSchedulePlanList {
   int32 jobid = 1;
   repeated TaskSchedulePlan taskscheduleplan = 2;
}
```

\`\`

### YAML file

The task scheduler has task.yaml in the config directory. The task scheduler mode represents either 
'roundrobin' or 'firstfit' or 'datalocalityaware'. The default task instances represents the default 
memory, disk, and cpu values assigned to the task instances. The default container padding values 
represents the percentage of values to be added to each container. The default container instance 
values represents the default size of memory, disk, and cpu of the container. The task parallelism 
represents the default parallelism value assigned to each task instance. The task type represents 
the streaming or batch task.The task scheduler dynamically loads the respective streaming and batch 
task schedulers based on the configuration values specified in the task.yaml.

\`\`yaml

```text
#Streaming Task Scheduler Mode "roundrobin" or  "firstfit" or "datalocalityaware"
twister2.streaming.taskscheduler: "roundrobin"

#Streaming Task Scheduler Class
twister2.streaming.taskscheduler.class: "edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler"

#Batch Task Scheduler Mode "roundrobin" or  "datalocalityaware"
twister2.batch.taskscheduler: "roundrobin"

#Batch Task Scheduler Class
twister2.batch.taskscheduler.class: "edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduler"

#Default Task Instance Values
twister2.task.instances: 2
twister2.task.instance.ram: 512.0
twister2.task.instance.disk: 500.0
twister2.task.instance.cpu: 2.0

#Default Container Padding Values
twister2.ram.padding.container: 2.0
twister2.disk.padding.container: 12.0
twister2.cpu.padding.container: 1.0
twister2.container.padding.percentage: 2

#Default Container Instance Values
twister2.container.instance.ram: 2048.0
twister2.container.instance.disk: 2000.0
twister2.container.instance.cpu: 4.0

#Default Task Parallelism Value
twister2.task.parallelism: 2

#Default Task Type "streaming" or "batch"
twister2.task.type: "streaming"
```

\`\`

### User-Defined Task Scheduler

The task scheduler in Twister2 supports the user-defined task scheduler. The user-defined task 
scheduler has to implement the ITaskScheduler interface. The user has to specify the task scheduler 
as "user-defined" with the corresponding "user-defined" task scheduler class name.

\`\`yaml

```text
#User-defined Streaming Task Scheduler
twister2.streaming.taskscheduler: "user-defined"

#User-defined Streaming Task Scheduler Class
twister2.streaming.taskscheduler.class: "edu.iu.dsc.tws.tsched.userdefined.UserDefinedTaskScheduler"
```

\`\`

### Other task schedulers and their respective class names

The other task schedulers and their respective class names are given below. The user have to specify
the respective scheduler mode and their corresponding class names.

\`\`yaml

```text
#Streaming Task Scheduler Mode "roundrobin" or  "firstfit" or "datalocalityaware"
twister2.streaming.taskscheduler: "roundrobin"

#Streaming Task Scheduler Class
twister2.streaming.taskscheduler.class: "edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler"

#twister2.streaming.taskscheduler.class: "edu.iu.dsc.tws.tsched.streaming.datalocalityaware.DataLocalityStreamingTaskScheduler"
#twister2.streaming.taskscheduler.class: "edu.iu.dsc.tws.tsched.streaming.firstfit.FirstFitStreamingTaskScheduler"

#Batch Task Scheduler Mode "roundrobin" or  "datalocalityaware"
#twister2.batch.taskscheduler: "roundrobin"
twister2.batch.taskscheduler: "datalocalityaware"

#Batch Task Scheduler Class
twister2.batch.taskscheduler.class: "edu.iu.dsc.tws.tsched.batch.datalocalityaware.DataLocalityBatchTaskScheduler"
#twister2.batch.taskscheduler.class: "edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduler"
```

\`\`


## Execution

Task executor is the component which is responsible for executing the task graph. A process model or 
a hybrid model with threads can be used for execution which is based on the system specification. 
It is important to handle both I/O and task execution within a single execution module so that the 
framework can achieve the best possible performance by overlapping I/O and computations. The 
execution is responsible for managing the scheduled tasks and activating them with data coming from 
the message layer. Unlike in MPI based applications where threads are created equal to the number of 
CPU cores, big data systems typically employ more threads than the cores available to facilitate I/O 
operations. 

* Task Executor is the component which is responsible for executing the tasks that are submitted 
  through the task scheduler in each worker
  * It uses threads to execute a given task plan.
  * It allows to run one or more executors run on each worker node
  * It will queue the tasks and execute the tasks based on the submitted order. 
* The task executor will receive the tasks as serialized objects and it will deserialize the objects 
  before processing them. 
* A thread pool will be maintained by the task executors to manage the core in an optimal manner. 
  * The size of the thread pool will be determined by the number of cores that are available to the executor. 

### Types of Task Executors

* Task Executor is implemented with two types of executors namely
  * Batch Sharing Task Executor
  * Streaming Sharing Task Executor
* Task Executor invokes the appropriate task executors based on the type of the task graph.
* Batch Sharing Task Executor terminate after the computation ends whereas Streaming Sharing Task 
  Executor runs continuously. 

### Task Executor Call

```text
public Executor(Config cfg, int wId, ExecutionPlan executionPlan, TWSChannel channel, 
OperationMode operationMode)
```

### Execution Graph

* Execution graph is a transformation of the user-defined task graph created by the framework to be 
  executed on the cluster.
* Execution graph will be scheduled to the available resource by the task scheduler.