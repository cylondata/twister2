Streaming DataLocality Aware scheduler
======================================

DataLocality Aware Task Scheduler allocates the task instances of the streaming task graph based on the 
locality of data. It calculates the distance between the worker nodes and the data nodes and
allocate the streaming task instances to the worker nodes which are closer to the data nodes i.e. it takes 
lesser time to transfer/access the input data file. The data transfer time is calculated based on the 
network parameters such as bandwidth, latency, and size of the input file.

It generates the task schedule plan which consists of the containers (container plan) and the 
allocation of task instances (task instance plan) on those containers. The size of the container 
(memory, disk, and cpu) and the task instances (memory, disk, and cpu) are homogeneous in nature. 
First, it computes the distance between the worker node and the datanodes and allocate the task 
instances into the logical container values and then it will calculate the required ram, disk, and 
cpu values for the task instances and the logical containers which is based on the task configuration 
values and the allocated worker values respectively. 

## Implementation 
 
The data locality aware task scheduler for scheduling streaming task graph is implemented in

    edu.iu.dsc.tws.tsched.streaming.datalocalityaware.DataLocalityStreamingTaskScheduler
    
which implements the interface
 
    edu.iu.dsc.tws.tsched.streaming.taskschedule.ITaskScheduler
           
and the methods are
    
    initialize(Config config)
    
    schedule(DataflowTaskGraph graph, WorkerPlan workerplan)
      
    dataLocalityAwareSchedulingAlgorithm(
          Set<Vertex> taskVertexSet, int numberOfContainers, WorkerPlan workerPlan, Config config)
          
    dataLocalityAwareSchedulingAlgorithm(
          Vertex taskVertex, int numberOfContainers, WorkerPlan workerPlan, Config config)
          
    calculationList(int index, Config config, Vertex vertex, WorkerPlan workerPlan,
          Map<Integer, List<InstanceId>> aMap,  int containerIndex,  int maxTaskInstPerContainer)

    Map<String, List<DataTransferTimeCalculator>> distanceCalculation(List<String> datanodesList, 
          WorkerPlan workers, int index, List<Integer> assignedWorkers)
                              
    List<DataTransferTimeCalculator> findBestWorkerNode(Vertex vertex, Map<String, 
          List<DataTransferTimeCalculator>> workerPlanMap) 
    
The DataLocalityStreamingTaskScheduler first initialize the ram, disk, and cpu values with default 
task instance values specified in the TaskSchedulerContext. The schedule method invokes the 
DataLocalityStreamingTaskScheduling algorithm to perform the data locality based task scheduling
algorithm. It first calculates three important parameters namely maximum task instances per 
container, container capacity, and total task instances in the task graph.

    int maxTaskInstancesPerContainer = TaskSchedulerContext.defaultTaskInstancesPerContainer(config);
    
    int containerCapacity = maxTaskInstancesPerContainer * numberOfContainers;
    
    int totalTask = taskAttributes.getTotalNumberOfInstances(taskVertexSet);
 
The first parameter represents the total number of task instances could be allocated to the container.
The second parameter represents the total capacity of the capacity (number of available containers 
and the task instances to be allocated to the container). The third parameter represents the total
number of task instances in the task graph (which is based on the number of total tasks and its 
parallelism). If the container capacity value is greater than total task instances, it is 
possible to schedule the task instances else, it will throw the task schedule exception and the 
task instances couldn't be scheduled to the available containers. 

The DataNodeLocatorUtils is a helper class which is implemented in
    
     edu.iu.dsc.tws.data.utils.DataNodeLocatorUtils
     
which is responsible for getting the datanode location of the input files in the Hadoop Distributed 
File System (HDFS). If the index value is 0, the algorithm first calculate the distance between the 
worker nodes and the data nodes and store it in the map. Next, the algorithm send the task vertex 
and the distance calculation map to find out the best worker node which is based on the calculated 
distance. The distance calculation method in the algorithm get the network parameters(such as 
bandwidth and latency) from the workers property (if the network property is not null) else, the 
default value is assigned from the TaskScheduler Context. Then, it will allocate the task instances 
of the task vertex to the worker (which has minimal distance), if the container/worker has reached 
the maximum number of task instances it will allocate the remaining task instances to the next 
container. Once the container has reached its maximum task instance value, then the container is added in 
the allocated workers list (which represents that container has reached their maximum capacity 
and we can't any more task instances into the container). The procedure is same if the index 
value is greater than 0, but, the only difference is it has to calculate the distance between the 
worker nodes (which are not in the allocated workers list) and the datanodes. Finally, the 
algorithm returns the datalocalityawareallocation map object which consists of container and the 
task instance assignment details.

The DataLocalityStreamingTaskScheduler assign the logical container size based on the default ram, 
disk, and cpu values specified in the TaskScheduler Context. The default configuration value 
for the container is given below.

      private static final String TWISTER2_RAM_PADDING_PER_CONTAINER 
                                                                = "twister2.ram.padding.container";
      
      private static final double TWISTER2_RAM_PADDING_PER_CONTAINER_DEFAULT = 2.0;
    
      private static final String TWISTER2_DISK_PADDING_PER_CONTAINER
                                                               = "twister2.disk.padding.container";
      
      private static final double TWISTER2_DISK_PADDING_PER_CONTAINER_DEFAULT = 12.0;
    
      private static final String TWISTER2_CPU_PADDING_PER_CONTAINER
                                                                = "twister2.cpu.padding.container";
      
      private static final double TWISTER2_CPU_PADDING_PER_CONTAINER_DEFAULT = 1.0;
      
Then, the schedule method unwraps the datalocalityawarecontainer instance map and finds out the task 
instances allocated to each container. Based on the task instances required ram, disk, and cpu it
creates the required container object. If the worker has required ram, disk, and cpu value then it 
assigns those values to the containers otherwise, it will assign the calculated value of required 
ram, disk, and cpu value to the containers. Finally, the schedule method pack the task instance
plan and the container plan into the task schedule plan and return the same. 


    
    
    