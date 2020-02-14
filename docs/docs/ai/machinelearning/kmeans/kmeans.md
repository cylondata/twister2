---
id: kmeans
title: K-Means
sidebar_label: K-Means
---

The need to process large am​​ounts of continuously arriving information has led to the exploration and 
application of big data analytics techniques. Likewise, the painstaking process of clustering numerous 
datasets containing large numbers of records with high dimensions calls for innovative methods. Traditional 
sequential clustering algorithms are unable to handle it. They are not scalable in relation to larger sizes
of data sets, and they are most often computationally expensive in memory space and time complexities. Yet, 
the parallelization of data clustering algorithms is paramount when dealing with big data. K-Means clustering 
is an iterative algorithm hence, it requires a large number of iterative steps to find an optimal solution, 
and this procedure increases the processing time of clustering. Twister2 provides a dataflow task graph 
approach to distribute the tasks in a parallel manner and aggregate the results which reduce the processing 
time of K-Means Clustering process.

## K-Means Clustering Implementation Details

The implementation details of k-means clustering in Twister2 is discussed below.

### DataObjectConstants

The constants which are used by the k-means algorithm to specify the number of workers, parallelism, 
dimension, size of datapoints, size of centroids, file system, number of iterations, datapoints, and 
centroids directory. 

```java
  public static final String WORKERS = "workers";
  public static final String DIMENSIONS = "dim";
  public static final String PARALLELISM_VALUE = "parallelism";
  public static final String SHARED_FILE_SYSTEM = "fShared";
  public static final String DSIZE = "dsize";
  public static final String CSIZE = "csize";
  public static final String DINPUT_DIRECTORY = "dinput";
  public static final String CINPUT_DIRECTORY = "cinput";
  public static final String OUTPUT_DIRECTORY = "output";
  public static final String NUMBER_OF_FILES = "nFiles";
  public static final String FILE_SYSTEM = "filesys";
  public static final String ITERATIONS = "iter";
```

### KMeansMain

The entry point for the K-Means clustering algorithm is implemented in KMeansMain class. It 
parses the command line parameters submitted by the user for running the K-Means clustering algorithm. 
It first set the submitted variables in the JobConfig object and put the JobConfig object into the 
Twister2Job Builder, set the worker class (KMeansComputeJob.java in this example) and submit the job. 

```java
edu.iu.dsc.tws.examples.batch.kmeans.KMeansMain
```

### KMeansComputeJob

It is the main class for the K-Means clustering which consists of four main tasks namely generation 
of datapoints and centroids, partition and read the partitioned data points, partition and read the 
centroids, and perform the distance calculation between the datapoints and the centroids. It extends
the IWorker class which has the execute() method that invokes the KMeansUtils class to generate the 
datapoints and the centroids in their respective filesystem and their directories. Then, the execute() 
method of KMeansComputeJob invokes "datapointsTaskgraph", "centroidsTaskGraph", and "kmeansTaskGraph". 
We will briefly discuss the functionalities of each task graph defined in the KMeansComputeJob. 

### Reading and partitioning the Datapoints
The main functionality of the first task graph is to partition the data points, convert the 
partitioned datapoints into two-dimensional array, and write the two-dimensional array into their 
respective task index values. 

```java
    /* First Graph to partition and read the partitioned data points **/
   PointDataSource ps = new PointDataSource(Context.TWISTER2_DIRECT_EDGE,
           dataDirectory, "points", dimension);
```

First, add point data source to the task graph builder for the first task graph. Then, set the 
operation mode and the task graph name.
 
```java
     datapointsComputeGraphBuilder.addSource("datapointsource", ps, parallelismValue);
     datapointsComputeGraphBuilder.setMode(OperationMode.BATCH);
     datapointsComputeGraphBuilder.setTaskGraphName("datapointsTG");
```

Finally, invoke the computeGraphBuilder to build the first task graph, get the task schedule plan and 
execution plan for the first task graph, and call the execute() method to execute the datapoints task 
graph.

```java
    //Build the first taskgraph
    DataFlowTaskGraph datapointsTaskGraph = computeGraphBuilder.build();
    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan); 
```

#### PointDataSource 

This class partition the datapoints which is based on the task parallelism value. It may either use
the "LocalTextInputPartitioner" or "LocalFixedInputPartitioner" to partition the datapoints. 
Finally, write the partitioned datapoints into their respective edges. The LocalTextInputPartitioner
partition the datapoints based on the block whereas the LocalFixedInputPartitioner partition the 
datapoints based on the length of the file. For example, if the task parallelism is 4, if there are 
16 datapoints each task will get 4 datapoints to process. The partitioned datapoints are converted
into two-dimensional array.  
 
```java
 @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    ExecutionRuntime runtime = (ExecutionRuntime) cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);
    this.source = runtime.createInput(cfg, context, new LocalTextInputPartitioner(
        new Path(getDataDirectory()), context.getParallelism(), config));
  }
```

Finally, write the appropriate datapoints into their respective task index values with the entity 
partition values.

```java
 @Override
  public DataPartition<double[][]> get() {
    return new EntityPartition<>(context.taskIndex(), dataPointsLocal);
  }
```

### Reading and partitioning the Centroids  

Similar to the datapoints, the second task graph perform three processes namely partitioning, 
converting the partitioned centroids into array, and writing into respective task index values 
but, with one major difference of read the complete file as one partition. 

```java
      PointDataSource cs = new PointDataSource(Context.TWISTER2_DIRECT_EDGE, centroidDirectory,
            "centroids", dimension);
      ComputeGraphBuilder centroidsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
```

Similar to the first task graph, it add point data source to the task graph builder for the 
second graph and then it set the operation mode and the task graph name.

```java
   centroidsComputeGraphBuilder.addSource("centroidsource", cs, parallelismValue);
   centroidsComputeGraphBuilder.setMode(OperationMode.BATCH);
   centroidsComputeGraphBuilder.setTaskGraphName("centTG");
```

Finally, invoke the build() method to build the second task graph, get the task schedule plan and 
execution plan for the second task graph, and call the execute() method to execute the centroids 
task graph. 

```java
    //Build the second taskgraph
    DataFlowTaskGraph centroidsTaskGraph = computeGraphBuilder.build();
    //Get the execution plan for the second task graph
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(centroidsTaskGraph);
    //Actual execution for the second taskgraph
    taskExecutor.execute(centroidsTaskGraph, secondGraphExecutionPlan); 
```

### KMeans Clustering 

The third task graph has the following classes namely KMeansSource, KMeansAllReduceTask, and 
CentroidAggregator. 

```java
    /* Third Graph to do the actual calculation **/
       KMeansSourceTask kMeansSourceTask = new KMeansSourceTask();
       KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();
       ComputeGraphBuilder kmeansComputeGraphBuilder = ComputeGraphBuilder.newBuilder(conf);
   
       //Add source, and sink tasks to the task graph builder for the third task graph
       kmeansComputeGraphBuilder.addSource("kmeanssource", kMeansSourceTask, parallelismValue);
       ComputeConnection kMeanscomputeConnection = kmeansComputeGraphBuilder.addCompute(
           "kmeanssink", kMeansAllReduceTask, parallelismValue);
   
       //Creating the communication edges between the tasks for the third task graph
       kMeanscomputeConnection.allreduce("kmeanssource")
           .viaEdge("all-reduce")
           .withReductionFunction(new CentroidAggregator())
           .withDataType(MessageTypes.OBJECT);
       kmeansComputeGraphBuilder.setMode(OperationMode.BATCH);
       kmeansComputeGraphBuilder.setTaskGraphName("kmeansTG");
```

#### Assigning datapoints and initial centroids

The datapoints and centroids are sent to the KMeansTaskGraph as "points" object and "centroids" 
object as an input for further processing through receivable name set. Finally, it invokes the 
execute() method of the task executor to do the clustering process.

```java
    //Perform the iterations from 0 to 'n' number of iterations
   IExecutor ex = taskExecutor.createExecution(kmeansTaskGraph);
    for (int i = 0; i < iterations; i++) {
         //actual execution of the third task graph
         ex.execute(i == iterations - 1);
    }
```

#### New Centroid Updation
This process repeats for ‘n’ number of iterations as specified by the user. For every iteration, the 
new centroid value is calculated and the calculated value is distributed across all the task instances. 
At the end of every iteration, the centroid value is updated and the iteration continues with the 
new centroid value.

### KMeansSourceTask 
First, the execute method in KMeansSource retrieve the partitioned data points into their respective 
task index values and the complete centroid values into their respective task index values. The retrieved 
data points and centroids are sent to the KMeansUtils to find the nearest centers using the Euclidean 
distance. 
 
```java
   @Override
   public void execute() {
      int dim = config.getIntegerValue("dim", 2);
      double[][] datapoints = (double[][]) dataPartition.first();
      double[][] centroid = (double[][]) centroidPartition.first();
      double[][] kMeansCenters = KMeansUtils.findNearestCenter(dim, datapoints, centroid);
      context.writeEnd("all-reduce", kMeansCenters);
   }
```
     
### KMeansAllReduceTask
The KMeansAllReduceTask write the calculated centroid values of their partitioned datapoints into their respective task index values.

```java
    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.FINE, "Received centroids: " + context.getWorkerId()
          + ":" + context.taskId());
      centroids = (double[][]) message.getContent();
      newCentroids = new double[centroids.length][centroids[0].length - 1];
      for (int i = 0; i < centroids.length; i++) {
        for (int j = 0; j < centroids[0].length - 1; j++) {
          double newVal = centroids[i][j] / centroids[i][centroids[0].length - 1];
          newCentroids[i][j] = newVal;
        }
      }
      return true;
    }

    @Override
    public DataPartition<double[][]> get() {
      return new EntityPartition<>(context.taskIndex(), newCentroids);
    }
```

### CentroidAggregator

The CentroidAggregator implements the IFunction and the function OnMessage which accepts two objects as an argument.

```java
public Object onMessage(Object object1, Object object2)
```

It sums the corresponding centroid values and return the same.

```java
ret.setCenters(newCentroids); 
```

## To Run K-Means Clustering using Task Graph
   
This command generate and write the datapoints and centroids in the local filesystem and run the 
K-Means clustering process. If the user wants to generate and do the processing with csv file, they
have to specify the csv file type as "-ftype csv" or else if the user wants to do the processing
with text file, they have to specify the file type as "-ftype txt".
   
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.kmeans.KMeansMain -dinput /tmp/dinput -cinput /tmp/cinput -fShared false -nFiles 1 -output /tmp/output -workers 2 -dim 2 -parallelism 4 -filesys local -dsize 1000 -csize 4 -iter 100 -type graph -ftype txt
```
      
This command generate and write the datapoints and centroids in the HDFS and run the run the 
K-Means clustering process. 
   
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.kmeans.KMeansMain -dinput hdfs://namenode:9000/tmp/dinput -cinput hdfs://namenode:9000/tmp/cinput -fShared false -nFiles 1 -output hdfs://namenode:9000/tmp/output -workers 2 -dim 2 -parallelism 4 -filesys hdfs -dsize 1000 -csize 4 -iter 100 -type graph -ftype txt
```

## To Run K-Means Clustering using TSet
   
This command generate and write the datapoints and centroids in the local filesystem and run the 
K-Means clustering process. 
   
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.kmeans.KMeansMain -dinput /tmp/dinput -cinput /tmp/cinput -fShared false -nFiles 1 -output /tmp/output -workers 2 -dim 2 -parallelism 4 -filesys local -dsize 1000 -csize 4 -iter 100 -type tset -ftype txt
```
      
This command generate and write the datapoints and centroids in the HDFS and run the run the 
K-Means clustering process. 
   
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.kmeans.KMeansMain -dinput hdfs://namenode:9000/tmp/dinput -cinput hdfs://namenode:9000/tmp/cinput -fShared false -nFiles 1 -output hdfs://namenode:9000/tmp/output -workers 2 -dim 2 -parallelism 4 -filesys hdfs -dsize 1000 -csize 4 -iter 100 -type tset -ftype txt
```

### Sample Output 

```bash
[2019-11-22 10:41:15 -0500] [INFO] [-] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPILauncher: Starting the job master: 127.0.1.1:44675  
[2019-11-22 10:41:15 -0500] [WARNING] [-] [main] edu.iu.dsc.tws.master.server.JobMaster: Dashboard host address is null. Not connecting to Dashboard  
[2019-11-22 10:41:15 -0500] [INFO] [-] [main] edu.iu.dsc.tws.common.net.tcp.Server: Starting server on kannan-Precision-5820-Tower-X-Series:44675  
[2019-11-22 10:41:15 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.JobMaster: JobMaster [127.0.1.1] started and waiting worker messages on port: 44675  
[2019-11-22 10:41:15 -0500] [INFO] [-] [Thread-4] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIController: Working directory: /home/kannan/.twister2/jobs  
[2019-11-22 10:41:15 -0500] [INFO] [-] [Thread-4] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIController: Launching job in standalone scheduler with no of containers = 2  
[2019-11-22 10:41:15 -0500] [INFO] [-] [Thread-4] edu.iu.dsc.tws.rsched.schedulers.standalone.MPICommand: Job class path: /home/kannan/.twister2/jobs/KMeans-job/libexamples-java.jar  
[2019-11-22 10:41:15 -0500] [INFO] [-] [Thread-4] edu.iu.dsc.tws.rsched.schedulers.standalone.StandaloneCommand: Java version : 8  
[2019-11-22 10:41:16 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerMonitor: Worker: 1 joined the job.  
[2019-11-22 10:41:16 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerMonitor: Worker: 0 joined the job.  
[2019-11-22 10:41:16 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerHandler: All workers joined the job. Worker IDs: [0, 1]  
[2019-11-22 10:41:16 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerHandler: Sending WorkersJoined messages ...  
[2019-11-22 10:41:17 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.batch.kmeans.KMeansComputeJob: Total K-Means Execution Time: 352	Data Load time : 109	Compute Time : 243  
[2019-11-22 10:41:17 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker: Worker finished executing - 0  
[2019-11-22 10:41:17 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerMonitor: Worker:0 COMPLETED.  
[2019-11-22 10:41:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.batch.kmeans.KMeansComputeJob: Total K-Means Execution Time: 354	Data Load time : 130	Compute Time : 224  
[2019-11-22 10:41:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker: Worker finished executing - 1  
[2019-11-22 10:41:17 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerMonitor: Worker:1 COMPLETED.  
[2019-11-22 10:41:17 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerMonitor: All 2 workers COMPLETED. Terminating the job.  
[2019-11-22 10:41:17 -0500] [INFO] [-] [main] edu.iu.dsc.tws.rsched.core.ResourceAllocator: CLEANED TEMPORARY DIRECTORY......:/tmp/twister2-KMeans-job-7364072149483599729  
```
