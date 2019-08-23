# Task Scheduler

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


## Implementation

The task scheduler has the main class named TaskScheduler which implements the base interface ITaskScheduler 
that consists of two main methods namely initialize and schedule methods to initialize the taskgraph 
configuration and schedule the taskgraph based on the workers information available in the resource plan.

```text
edu.iu.dsc.tws.api.compute.schedule.ITaskScheduler
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

## Proto file

The task scheduler has the protobuf file named taskscheduleplan.proto in the proto directory. It 
considers both the soft (CPU, disk) and hard (RAM) constraints which generates the task schedule plan 
in the format of Google Protobuf object. The proto file consists of the following details as follows. 
The resource object represents the values of cpu, memory, and disk of the resources. The task instance 
plan holds the task id, task name, task index, and container object. The container plan consists of 
container id, task instance plan, required and scheduled resource of the container. The task schedule 
plan holds the jobid or the taskgraph id and the container plan. The task schedule plan list is mainly 
responsible for holding the taskschedule of the batch tasks.

```text
message Resource { 
   double availableCPU = 1; 
   double availableMemory = 2; 
   double availableDisk = 3; 
}

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

## YAML file

The task scheduler has task.yaml in the config directory. The task scheduler mode represents either 
'roundrobin' or 'firstfit' or 'datalocalityaware'. The default task instances represents the default 
memory, disk, and cpu values assigned to the task instances. The default container padding values 
represents the percentage of values to be added to each container. The default container instance 
values represents the default size of memory, disk, and cpu of the container. The task parallelism 
represents the default parallelism value assigned to each task instance. The task type represents 
the streaming or batch task. The task scheduler dynamically loads the respective streaming and batch 
task schedulers based on the configuration values specified in the task.yaml.

```text
# Task scheduling mode for the streaming jobs "roundrobin" or "firstfit" or "datalocalityaware" or "userdefined"
twister2.taskscheduler.streaming: "roundrobin"

# Task Scheduler class for the round robin streaming task scheduler
twister2.taskscheduler.streaming.class: "edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler"

# Task scheduling mode for the batch jobs "roundrobin" or "datalocalityaware" or "userdefined" or "batchscheduler"
twister2.taskscheduler.batch: "roundrobin"

# Task Scheduler class for the round robin batch task scheduler
twister2.taskscheduler.batch.class: "edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduler"

# Number of task instances to be allocated to each worker/container
twister2.taskscheduler.task.instances: 2

# Ram value to be allocated to each task instance
twister2.taskscheduler.task.instance.ram: 512.0

# Disk value to be allocated to each task instance
twister2.taskscheduler.task.instance.disk: 500.0

# CPU value to be allocated to each task instancetwister2.task.parallelism
twister2.taskscheduler.instance.cpu: 2.0

# Default Container Instance Values
# Ram value to be allocated to each container
twister2.taskscheduler.container.instance.ram: 4096.0

# Disk value to be allocated to each container
twister2.taskscheduler.container.instance.disk: 8000.0

twister2.taskscheduler.container.instance.cpu: 16.0

# Default Container Padding Values
# Default padding value of the ram to be allocated to each container
twister2.taskscheduler.ram.padding.container: 2.0

# Default padding value of the disk to be allocated to each container
twister2.taskscheduler.disk.padding.container: 12.0

# CPU padding value to be allocated to each container
twister2.taskscheduler.cpu.padding.container: 1.0

# Percentage value to be allocated to each container
twister2.taskscheduler.container.padding.percentage: 2

# Static Default Network parameters
# Bandwidth value to be allocated to each container instance for datalocality scheduling
twister2.taskscheduler.container.instance.bandwidth: 100 #Mbps

# Latency value to be allocated to each container instance for datalocality scheduling
twister2.taskscheduler.container.instance.latency: 0.002 #Milliseconds

# Bandwidth to be allocated to each datanode instance for datalocality scheduling
twister2.taskscheduler.datanode.instance.bandwidth: 200 #Mbps

# Latency value to be allocated to each datanode instance for datalocality scheduling
twister2.taskscheduler.datanode.instance.latency: 0.01 #Milliseconds

# Prallelism value to each task instance
twister2.taskscheduler.task.parallelism: 2

# Task type to each submitted job by default it is "streaming" job.
twister2.taskscheduler.task.type: "streaming"

# number of threads per worker
twister2.exector.worker.threads: 1

# name of the batch executor
twister2.executor.batch.name: "edu.iu.dsc.tws.executor.threading.BatchSharingExecutor2"

# number of tuples executed at a single pass
twister2.exector.instance.queue.low.watermark: 10000
```

## User-Defined Task Scheduler

The task scheduler in Twister2 supports the user-defined task scheduler. The user-defined task 
scheduler has to implement the ITaskScheduler interface. The user has to specify the task scheduler 
as "user-defined" with the corresponding "user-defined" task scheduler class name.

\`\`yaml

```text
#User-defined Streaming Task Scheduler
twister2.streaming.taskscheduler: "user-defined"

# Task Scheduler for the userDefined Streaming Task Scheduler
#twister2.taskscheduler.streaming.class: "edu.iu.dsc.tws.tsched.userdefined.UserDefinedTaskScheduler"

# Task Scheduler for the userDefined Batch Task Scheduler
#twister2.taskscheduler.batch.class: "edu.iu.dsc.tws.tsched.userdefined.UserDefinedTaskScheduler"
```

\`\`

## Batch Task Scheduler 

Batch Task Scheduler is able to handle and schedule both single task graph as well as multiple 
dependent task graphs. The main constraint considered in the batch task scheduler is specify the same 
parallelism value for the dependent tasks in the task graphs. It schedule the tasks in a round
robin fashion but, while scheduling the child or the dependent tasks it considers the data locality 
of the input data from the parent tasks and schedule the tasks in a round robin fashion to the workers. 

\`\`yaml

```text
#Batch Task Scheduler
twister2.taskscheduler.batch: "batchscheduler"

#Task Scheduler class for the batch task scheduler
twister2.taskscheduler.batch.class: "edu.iu.dsc.tws.tsched.batch.batchscheduler.BatchTaskScheduler"
```

\`\`

## Other task schedulers and their respective class names

The other task schedulers and their respective class names are given below. The user have to specify
the respective scheduler mode and their corresponding class names.


\`\`yaml

```text
# Task Scheduler for the Data Locality Aware Streaming Task Scheduler
#twister2.taskscheduler.streaming.class: "edu.iu.dsc.tws.tsched.streaming.datalocalityaware.DataLocalityStreamingTaskScheduler"

# Task Scheduler for the FirstFit Streaming Task Scheduler
#twister2.taskscheduler.streaming.class: "edu.iu.dsc.tws.tsched.streaming.firstfit.FirstFitStreamingTaskScheduler"

# Task Scheduler for the Data Locality Aware Batch Task Scheduler
#twister2.taskscheduler.batch.class: "edu.iu.dsc.tws.tsched.batch.datalocalityaware.DataLocalityBatchTaskScheduler"
```

\`\`

