Batch DataLocality Aware Task Scheduler 
========================================

DataLocality Aware Task Scheduler allocates the task instances of the streaming task graph based on the 
locality of data. It calculates the distance between the worker nodes and the data nodes and allocate 
the batch task instances to the worker nodes which are closer to the data nodes i.e. it takes lesser
time to transfer/access the input data file. The data transfer time is calculated based on the 
network parameters such as bandwidth, latency, and size of the input file. It generates the task 
schedule plan which consists of the containers (container plan) and the allocation of task instances 
(task instance plan) on those containers. The size of the container (memory, disk, and cpu) and the 
task instances (memory, disk, and cpu) are homogeneous in nature. First, it computes the distance 
between the worker node and the datanodes and allocate the task instances into the logical container
values and then it will calculate the required ram, disk, and cpu values for the task instances and 
the logical containers which is based on the task configuration values and the allocated worker 
values respectively. 

## Implementation 
 
The data locality aware task scheduler for scheduling batch task graph is implemented in

    edu.iu.dsc.tws.tsched.batch.datalocalityaware.DataLocalityBatchTaskScheduler
    
which implements the interface
 
    edu.iu.dsc.tws.tsched.streaming.taskschedule.ITaskScheduler
           
and the methods are
    
    initialize(Config config)
    
    schedule(DataflowTaskGraph graph, WorkerPlan workerplan)
    
     Map<Integer, List<InstanceId>> dataLocalityBatchSchedulingAlgorithm(
            Vertex taskVertex, int numberOfContainers, WorkerPlan workerPlan, Config config)
                      
     Map<Integer, List<InstanceId>> dataLocalityBatchSchedulingAlgorithm(
            Set<Vertex> taskVertexSet, int numberOfContainers, WorkerPlan workerPlan, Config config) 
              
     Map<String, List<DataTransferTimeCalculator>> distanceCalculation(
            List<String> datanodesList, WorkerPlan workers, int taskIndex)
              
     List<DataTransferTimeCalculator> findBestWorkerNode(Vertex vertex, Map<String,
            List<DataTransferTimeCalculator>> workerPlanMap)
    
The DataLocalityBatchTaskScheduler first initialize the ram, disk, and cpu values with default task 
instance values specified in the TaskSchedulerContext. The schedule method gets the set of task 
vertices/task vertex of the taskgraph and send that value and the number of workers to  the 
DataLocalityBatchScheduling algorithm. The algorithm first calculate the total number of task 
instances could be allocated to the container as given below:

    int maxTaskInstancesPerContainer = TaskSchedulerContext.defaultTaskInstancesPerContainer(config);
    
Next, the algorithm retrieve the total number of task instances from the Task Attributes for the 
particular task. Based on the maxTaskInstancesPerContainer value, the algorithm allocates the task 
instances into the respective container. The algorithm uses the DataNodeLocatorUtils which is a 
helper class and it is implemented in
    
     edu.iu.dsc.tws.data.utils.DataNodeLocatorUtils
     
which is responsible for getting the datanode location of the input files in the Hadoop Distributed 
File System (HDFS). 

The algorithm send the task vertex and the distance calculation map to find out the best worker node
which is calculated between the worker nodes and the data nodes and store it in the map. Then, it 
allocate the task instances of the task vertex to the worker (which has minimal distance), if the 
container/worker has reached the maximum number of task instances then it will allocate the remaining
task instances to the next container. Finally, the algorithm returns the datalocalityawareallocation
map object which consists of container and its task instance allocation.

The DataLocalityBatchTaskScheduler assign the logical container size which is based on the default 
ram, disk, and cpu values specified in the TaskScheduler Context. The default configuration value 
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
      
Then, the algorithm unwraps the datalocalityawarecontainer instance map and finds out the task 
instances allocated to each container. Based on the task instances required ram, disk, and cpu it 
creates the required container object. If the worker has required ram, disk, and cpu value then it 
assigns those values to the containers otherwise, it will assign the calculated value of required 
ram, disk, and cpu value to the containers. Finally, the algorithm pack the task instance 
plan and the container plan into the task schedule plan and return the same. 