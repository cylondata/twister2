Streaming FirstFit Task Scheduler
=================================

FirstFit Task Scheduler allocates the task instances of the task graph in a heuristic 
manner. The main objective of the task scheduler is to reduce the total number of containers and
support the heterogeneous containers and task instances allocation.

For example, if there are two tasks with parallelism value of 2, 1st task -> instance 0 will
go to container 0, 1st task -> instance 1 will go to container 0, 2nd task -> instance 0 will
go to container 0 (if the total task instance required values doesn't reach the maximum size of
container 0. If the container has reached its maximum limit then it will allocate the
2nd task -> instance 1 will go to container 1.

## Implementation

The first fit task scheduler for scheduling streaming task is implemented in 

    edu.iu.dsc.tws.tsched.streaming.firstfit.FirstFitStreamingTaskScheduler
    
which implements the interface
 
    edu.iu.dsc.tws.tsched.streaming.taskschedule.ITaskScheduler
    
The initialize method in the FirstFitStreamingTaskScheduler first initialize the task instance ram, disk, 
and cpu values with default task instance values specified in the TaskSchedulerContext. Next, it assign the 
logical container size based on the default ram, disk, and cpu values specified in the TaskScheduler Context. 
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
      
The schedule method invokes the firstfitTaskScheduling method for the logical allocation of the
task instances to the logical container. The assignInstancesToContainers method receive the parallel 
task map and it first sorts the task map based on the requirements of the ram value. It allocates
the task instances into the first container which has enough resources otherwise, it will allocate 
a new container to allocate the task instances.  
 