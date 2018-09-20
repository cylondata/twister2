Task Scheduling in Twister2
===========================

.. toctree::
   :maxdepth: 5
   
   streaming/data-locality-streaming-task-scheduler
   streaming/first-fit-streaming-task-scheduler
   streaming/round-robin-task-scheduler
   batch/round-robin-batch-task-scheduler
   batch/data-locality-batch-task-scheduler
   

Task Scheduling is the process of scheduling the tasks into the cluster resources in a manner that 
minimizes the task completion time and utilizes the resources effectively. The other main functional 
requirements of task scheduling are scalability, dynamism, time and cost efficiency, handling 
different types of processing models, data, and jobs, etc. The main objective of the task scheduling 
is scheduling the task instances in an appropriate order to suitable/appropriate cluster nodes. 
The task scheduling algorithms are broadly classified into two types namely static task scheduling 
algorithms and dynamic task scheduling algorithms. The task scheduler in twister2 is designed in a 
way such that it is able to handle both streaming and batch tasks. It supports the following task 
scheduling algorithms namely RoundRobin, FirstFit, and DataLocality. 

**RoundRobin Task Scheduling & DataLocality Aware Task Scheduling**
* Homogeneous Containers -> Homogeneous Task Instances

**FirstFit Task Scheduling**
* Heterogeneous Containers -> Heterogeneous Task Instances

## Implementation

The task scheduler has the main class named TaskScheduler which implements the base interface  
ITaskScheduler that consists of two main methods namely initialize and schedule methods to initialize
the taskgraph configuration and schedule the taskgraph based on the workers information available in
the resource plan.
          
    edu.iu.dsc.tws.tsched.spi.taskschedule.ITaskScheduler
    
has the following methods namely
    
    initialize (Config config)
    
    schedule(DataflowTaskGraph graph, WorkerPlan plan)
    
The TaskScheduler is responsible for invoking the respective schedulers based on the type of task 
and the scheduling mode. If it is a streaming task, it will invoke the scheduleStreamingTask method 
whereas if it is a batch task it will invoke the scheduleBatchTask method.

    edu.iu.dsc.tws.tsched.taskscheduler.TaskScheduler
    
has the following methods such as

    scheduleStreamingTask()
    
    scheduleBatchTask()
    
    generateTaskSchedulePlan(String classname)
    
The scheduleStreamingTask and scheduleBatchTask call the generateTaskSchedulePlan with the scheduling
class name as an argument which is specified in the task.yaml. The generateTaskSchedulePlan 
dynamically load the respective task schedulers (roundrobin, firstfit, or datalocalityaware) and 
access the initialize and schedule methods in the task schedulers to generate the task schedule plan.

## Proto file

The task scheduler has the protobuf file named taskscheduleplan.proto in the proto directory. The
proto file consists of the following details as follows. The resource object represents the values 
of cpu, memory, and disk of the resources. The task instance plan holds the task id, task name, task 
index, and container object. The container plan consists of container id, task instance plan, 
required and scheduled resource of the container. The task schedule plan holds the jobid or the
taskgraph id and the container plan. The task schedule plan list is mainly responsible for holding 
the taskschedule of the batch tasks. 

``bash
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
``

## YAML file
The task scheduler has task.yaml in the config directory. The task scheduler mode represents either 
'roundrobin' or 'firstfit' or 'datalocalityaware'. The default task instances represents the 
default memory, disk, and cpu values assigned to the task instances. The default container padding 
values represents the percentage of values to be added to each container. The default container 
instance values represents the default size of memory, disk, and cpu of the container. The task
parallelism represents the default parallelism value assigned to each task instance. The task type 
represents the streaming or batch task.
 
``yaml   
    #Task Scheduler Mode
    twister2.class.task.taskscheduler: "roundrobin"
    #twister2.class.task.taskscheduler: "firstfit"
    #twister2.class.task.taskscheduler:  "datalocalityaware"
    
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
 ``