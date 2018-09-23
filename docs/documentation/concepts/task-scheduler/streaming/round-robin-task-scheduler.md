Streaming RoundRobin Task Scheduler
===================================

RoundRobin Task Scheduler allocates the task instances of the task graph in a round robin fashion. 
For example, if there are 2 containers and 2 tasks with a task parallelism value of 2, 
task instance 0 of 1st task will go to container 1 and task instance 0 of 2nd task will go to 
container 1 whereas task instance 1 of 1st task will go to container 1 and task instance 1 of 2nd 
task will go to container 2.

It generates the task schedule plan which consists of the containers (container plan) and the 
allocation of task instances (task instance plan) on those containers. The size of the container 
(memory, disk, and cpu) and the task instances (memory, disk, and cpu) are homogeneous in nature. 

First, it will allocate the task instances into the logical container values and then it will 
calculate the required ram, disk, and cpu values for the task instances and the logical containers 
which is based on the task configuration values and the allocated worker values respectively. 

## Implementation 
 
The round robin task scheduler for scheduling streaming task is implemented in

    edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler
    
which implements the interface
 
    edu.iu.dsc.tws.tsched.streaming.taskschedule.ITaskScheduler
           
and the methods are
    
         initialize(Config config)
         schedule(DataflowTaskGraph graph, WorkerPlan workerplan)
    
The initialize method in the RoundRobinTaskScheduler first initialize the task instance ram, disk, 
and cpu values with default task instance values specified in the TaskSchedulerContext. The 
schedule method first gets the task vertex set of the taskgraph and send that task vertex set and 
the number of workers to the roundRobinSchedulingAlgo method for the logical allocation of the
task instances to the logical container in a round robin fashion. Next, it assign the 
logical container size based on the default ram, disk, and cpu values specified in the 
TaskScheduler Context. The default configuration value for the container is given below.

      private static final String TWISTER2_RAM_PADDING_PER_CONTAINER 
      = "twister2.ram.padding.container";
      
      private static final double TWISTER2_RAM_PADDING_PER_CONTAINER_DEFAULT = 2.0;
    
      private static final String TWISTER2_DISK_PADDING_PER_CONTAINER
          = "twister2.disk.padding.container";
      
      private static final double TWISTER2_DISK_PADDING_PER_CONTAINER_DEFAULT = 12.0;
    
      private static final String TWISTER2_CPU_PADDING_PER_CONTAINER
          = "twister2.cpu.padding.container";
      
      private static final double TWISTER2_CPU_PADDING_PER_CONTAINER_DEFAULT = 1.0;
      
      
Then, the algorithm unwraps the container instance map and finds out 
the task instances allocated to each container. Based on the task instance required ram, disk, and cpu it
creates the required container object. If the worker has required ram, disk, and cpu value then it 
assigns those values to the containers otherwise, it will assign the calculated value of required 
ram, disk, and cpu value to the containers. Finally, the algorithm method pack the task 
instance plan and the container plan into the task schedule plan and return the same. 


    
    
    