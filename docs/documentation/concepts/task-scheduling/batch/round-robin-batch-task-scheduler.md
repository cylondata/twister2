Batch RoundRobin TaskScheduler
===============================

RoundRobinBatchTaskScheduler allocates the task instances of the task graph in a round robin 
fashion which is similar to the Round Robin Task Scheduler for batch tasks. However, the primary 
difference between the streaming and batch task scheduling is, the streaming tasks has been considered
as a complete task graph whereas the taskgraph for batch tasks has been divided into batches based on 
the level and the dependency of the tasks in the taskgraph. The sample batch taskgraph 
example is given below. 

          Source (Task 1)
           |
           |
           V
        Task 2 (Two Outgoing Edges)
        |     |
        |     |
        V     V
      Task 3  Task 4
           |
           |
           V
         Target (Task 5)
     
For the above task graph example, the tasks are divided into the following batches and scheduled 
into the available workers as given below:
    
    **Schedule Batches**
    
    1st batch --> Source
    2nd batch --> Task 2
    3rd batch --> Task 3 & Task 4
    4th batch --> Target

For example, if there are 2 containers and 4 batches of tasks (dependency tasks) with a task parallelism 
value of 2, task instance 0 of 1st task (Task 1) will go to container 0 and task instance 1 of 1st 
task will go to container 1,  task instance 0 of 2nd task (Task 2) will go to container 0 and task 
instance 1 of 2nd task will go to container 1, whereas task instance 0 of 3rd task (Task 3) will go 
to container 0 and task instance 0 of 4th task(Task 4) will go to container 0 and task instance 1 of
3rd task (Task 3) will go to container 1 and task instance 2 of 4th task (Task 4) will go to 
container 1. Finally, task instance 0 of 4th task will go to container 0 and task instance 1 of 4th 
task will go to container 1. At a time, a batch of task(s) (either single task or multiple tasks)
takes part in the execution. 

It generates the task schedule plan which consists of multiple containers (container plan) and the 
allocation of task instances (task instance plan) on those containers. The size of the container 
(memory, disk, and cpu) and the task instances (memory, disk, and cpu) are homogeneous in nature. 

First, it will allocate the task instances into the logical container values and then it will 
calculate the required ram, disk, and cpu values for the task instances and the logical containers 
which is based on the task configuration values and the allocated worker values respectively. 

## Implementation 
 
The round robin task scheduler for scheduling batch task is implemented in

    edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduler
    
which implements the interface
 
    edu.iu.dsc.tws.tsched.batch.taskschedule.ITaskScheduler
    
and the methods are

     initialize(Config config)
     schedule(DataflowTaskGraph graph, WorkerPlan workerplan)
    
The initialize method in the RoundRobinBatchTaskScheduler first initialize the task instance ram, 
disk, and cpu values with default task instance values specified in the TaskSchedulerContext. The 
TaskVertexParser acts as a helper class which is responsible for parsing the simple to complex 
batch task graph. It parses the task vertex set of the task graph and identify the source, parent, 
child, and target tasks and store the identified batch of tasks in a separate set. 

The algorithm (schedule method) first gets the task vertex set of the taskgraph and send the task 
vertex set and the number of workers to the roundRobinSchedulingAlgorithm for the task 
instances allocation to the logical container in a round robin fashion. Then, it allocates the logical 
container size based on the default ram, disk, and cpu values specified in the TaskScheduler Context. 
The default configuration value for the container is given below.

      private static final String TWISTER2_RAM_PADDING_PER_CONTAINER 
      = "twister2.ram.padding.container";
      
      private static final double TWISTER2_RAM_PADDING_PER_CONTAINER_DEFAULT = 2.0;
    
      private static final String TWISTER2_DISK_PADDING_PER_CONTAINER
          = "twister2.disk.padding.container";
      
      private static final double TWISTER2_DISK_PADDING_PER_CONTAINER_DEFAULT = 12.0;
    
      private static final String TWISTER2_CPU_PADDING_PER_CONTAINER
          = "twister2.cpu.padding.container";
      
      private static final double TWISTER2_CPU_PADDING_PER_CONTAINER_DEFAULT = 1.0;
      
      
The schedule method unwraps the roundrobincontainer instance map and finds out the task 
instances allocated to each container. Based on the required ram, disk, and cpu of the required
task instances it creates the required container object. If the worker has required ram, disk, and 
cpu value then it assigns those values to the containers otherwise, it will assign the calculated 
value of required ram, disk, and cpu value to the containers. Finally, the schedule method pack the 
task instance plan and the container plan into the task schedule plan and return the same. 