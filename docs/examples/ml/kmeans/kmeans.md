# K-Means Clustering 
The need to process large am​​ounts of continuously arriving information has led to the exploration and application of big data analytics techniques. Likewise, the painstaking process of clustering numerous datasets containing large numbers of records with high dimensions calls for innovative methods. 
Traditional sequential clustering algorithms are unable to handle it. They are not scalable in relation 
to larger sizes of data sets, and they are most often computationally expensive in memory space and time complexities. Yet, the parallelization of data clustering algorithms is paramount when dealing 
with big data. K-Means clustering is an iterative algorithm hence, it requires a large number of iterative steps to find an optimal solution, and this procedure increases the processing time of clustering. Twister2 provides a dataflow task graph approach to distribute the tasks in a parallel manner 
and aggregate the results which reduce the processing time of K-Means Clustering process.

## K-Means Clustering Implementation Details

The implementation details of k-means clustering in Twister2 is pictorially represented in Fig.1.

![K-Means Implementation](../../../images/kmeans.png)

### DataObjectConstants

The constants which are used by the k-means algorithm to specify the number of workers, parallelism, dimension, size of datapoints,
size of centroids, file system, number of iterations, datapoints and centroids directory. 

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

### KMeansWorkerMain

The entry point for the K-Means clustering algorithm is implemented in KMeansWorkerMain class. It 
parses the command line parameters submitted by the user for running the K-Means clustering algorithm. 
It first sets the submitted variables in the JobConfig object and put the JobConfig object into the 
Twister2Job Builder, set the worker class (KMeansWorker.java in this example) and submit the job. 

```java
edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerMain
```

### KMeansWorker

It is the main class for the K-Means clustering which consists of four main tasks namely generation 
of datapoints and centroids, partition and read the partitioned data points, partition and read the 
centroids, and perform the distance calculation between the datapoints and the centroids. It extends
the TaskWorker class which has the execute() method, the execute() method first invokes the 
KMeansWorkerUtils class to generate the datapoints and the centroids in their respective filesystem 
and their directories. Then, the execute() method of KMeansWorker invokes "datapointsTaskgraph", 
"centroidsTaskGraph", and "kmeansTaskGraph". We will briefly discuss the functionalities of each 
task graph defined in the KMeansWorker.

#### Datapoints partition and read the partitioned datapoints
The main functionality of the first task graph is to partition the data points, convert the 
partitioned datapoints into two-dimensional array, and write the two-dimensional array into their 
respective task index values. 

```java
    /* First Graph to partition and read the partitioned data points **/
   DataObjectSource dataObjectSource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
           dataDirectory);
   KMeansDataObjectCompute dataObjectCompute = new KMeansDataObjectCompute(
           Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue, dimension);
   KMeansDataObjectDirectSink dataObjectSink = new KMeansDataObjectDirectSink();
```

First, add the source, compute, and sink tasks to the task graph builder for the first task graph. 
Then, create the communication edges between the tasks for the first task graph.
 
```java
    taskGraphBuilder.addSource("datapointsource", dataObjectSource, parallelismValue);
    ComputeConnection datapointComputeConnection = taskGraphBuilder.addCompute("datapointcompute",
        dataObjectCompute, parallelismValue);
    ComputeConnection firstGraphComputeConnection = taskGraphBuilder.addSink("datapointsink",
        dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the first task graph
    datapointComputeConnection.direct("datapointsource", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    firstGraphComputeConnection.direct("datapointcompute", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);
```

Finally, invoke the taskGraphBuilder to build the first task graph, get the task schedule plan and execution plan for the first task graph, and call the execute() method to execute the datapoints task graph. Once the execution is finished, the output values are retrieved in the "datapointsObject".

```java
    //Build the first taskgraph
    DataFlowTaskGraph datapointsTaskGraph = taskGraphBuilder.build();
    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, "datapointsink");
```
#### DataObjectSource

This class partition the datapoints which is based on the task parallelism value. It may use 
either the "LocalTextInputPartitioner" or "LocalFixedInputPartitioner" to partition the datapoints. 
Finally, write the partitioned datapoints into their respective edges. The LocalTextInputPartitioner
 partition the datapoints based on the block whereas the LocalFixedInputPartitioner partition the 
 datapoints based on the length of the file. For example, if the task parallelism is 4, if there are 16 data points each task will get 4 datapoints to process. 

```java
 @Override
  public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    ExecutionRuntime runtime = (ExecutionRuntime) cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);
    this.source = runtime.createInput(cfg, context, new LocalTextInputPartitioner(
        new Path(getDataDirectory()), context.getParallelism(), config));
  }
```

#### KMeansDataObjectCompute

This class receives the partitioned datapoints as "IMessage" and convert those datapoints into 
two-dimensional for the k-means clustering process. The converted datapoints are send to the 
KMeansDataObjectDirectSink through "direct" edge.

```java
 while (((Iterator) message.getContent()).hasNext()) {
        String val = String.valueOf(((Iterator) message.getContent()).next());
        String[] data = val.split(",");
        for (int i = 0; i < getDimension(); i++) {
          datapoint[value][i] = Double.parseDouble(data[i].trim());
        }
        value++;
        context.write(getEdgeName(), datapoint);
      }
```

#### KMeansDataObjectDirectSink

This class receives the message object from the DataObjectCompute and write into their respective 
task index values. First, it store the iterator values into the array list then it convert the array
list values into double array values.

```java
 @Override
   public boolean execute(IMessage message) {
     List<double[][]> values = new ArrayList<>();
     while (((Iterator) message.getContent()).hasNext()) {
       values.add((double[][]) ((Iterator) message.getContent()).next());
     }
     dataPointsLocal = new double[values.size()][];
     for (double[][] value : values) {
       dataPointsLocal = value;
     }
     return true;
   }
``` 

Finally, write the appropriate data points into their respective task index values with the entity 
partition values.

```java
 @Override
  public DataPartition<double[][]> get() {
    return new EntityPartition<>(context.taskIndex(), dataPointsLocal);
  }
```

#### Centroids partition and read the partitioned centroids

Similar to the datapoints, the second task graph performs three processes namely partitioning, 
converting the partitioned centroids into array, and writing into respective task index values 
but, with one major difference of read the complete file as one partition. 

 1. DataFileReplicatedReadSource
 2. KMeansDataObjectCompute, and
 3. KMeansDataObjectDirectSink 
 
```java
     DataFileReplicatedReadSource dataFileReplicatedReadSource = new DataFileReplicatedReadSource(
           Context.TWISTER2_DIRECT_EDGE, centroidDirectory);
     KMeansDataObjectCompute centroidObjectCompute = new KMeansDataObjectCompute(
           Context.TWISTER2_DIRECT_EDGE, csize, dimension);
     KMeansDataObjectDirectSink centroidObjectSink = new KMeansDataObjectDirectSink();
```

Similar to the first task graph, it add the source, compute, and sink tasks to the task graph builder for the second task graph. Then, create the communication edges between the tasks for the second task graph.

```java
    //Add source, compute, and sink tasks to the task graph builder for the second task graph
    taskGraphBuilder.addSource("centroidsource", dataFileReplicatedReadSource, parallelismValue);
    ComputeConnection centroidComputeConnection = taskGraphBuilder.addCompute("centroidcompute",
        centroidObjectCompute, parallelismValue);
    ComputeConnection secondGraphComputeConnection = taskGraphBuilder.addSink(
        "centroidsink", centroidObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    centroidComputeConnection.direct("centroidsource", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    secondGraphComputeConnection.direct("centroidcompute", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);
```

Finally, invoke the build() method to build the second task graph, get the task schedule plan and execution plan for the second task graph, and call the execute() method to execute the centroids task graph. Once the execution is finished, the output values are retrieved in the "centroidsDataObject".

```java
    //Build the second taskgraph
    DataFlowTaskGraph centroidsTaskGraph = taskGraphBuilder.build();
    //Get the execution plan for the second task graph
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(centroidsTaskGraph);
    //Actual execution for the second taskgraph
    taskExecutor.execute(centroidsTaskGraph, secondGraphExecutionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> centroidsDataObject = taskExecutor.getOutput(
        centroidsTaskGraph, secondGraphExecutionPlan, "centroidsink");
```

#### DataFileReplicatedReadSource

This class uses the "LocalCompleteTextInputParitioner" to read the whole file from the centroids 
 directory and write into their task respective task index values using the "direct" task edge. 
 For example, if the size of centroid value is 16, each task index receive 16 centroid values completely. 
 
```java
 public void prepare(Config cfg, TaskContext context) {
    super.prepare(cfg, context);
    ExecutionRuntime runtime = (ExecutionRuntime) cfg.get(ExecutorContext.TWISTER2_RUNTIME_OBJECT);
    this.source = runtime.createInput(cfg, context, new LocalCompleteTextInputPartitioner(
          new Path(getDataDirectory()), context.getParallelism(), config));
  }
```

#### KMeans Clustering 

The third task graph has the following classes namely KMeansSource, KMeansAllReduceTask, and 
CentroidAggregator. Similar to the first and second task graph, first we have to add the source, 
sink, and communication edges to the third task graph. 

```java
    /* Third Graph to do the actual calculation **/
    KMeansSourceTask kMeansSourceTask = new KMeansSourceTask();
    KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();

    //Add source, and sink tasks to the task graph builder for the third task graph
    taskGraphBuilder.addSource("kmeanssource", kMeansSourceTask, parallelismValue);
    ComputeConnection kMeanscomputeConnection = taskGraphBuilder.addSink(
        "kmeanssink", kMeansAllReduceTask, parallelismValue);

    //Creating the communication edges between the tasks for the third task graph
    kMeanscomputeConnection.allreduce("kmeanssource", "all-reduce",
        new CentroidAggregator(), DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph kmeansTaskGraph = taskGraphBuilder.build();
```

#### Assigning datapoints and initial centroids

The datapoint and centroid values are sent to the KMeansTaskGraph as "points" object and "centroids" 
object as an input for further processing. Finally, it invokes the execute() method of the task 
executor to do the clustering process.

```java
    //Perform the iterations from 0 to 'n' number of iterations
    for (int i = 0; i < iterations; i++) {
      ExecutionPlan plan = taskExecutor.plan(kmeansTaskGraph);
      //add the datapoints and centroids as input the kmeanssource task.
      taskExecutor.addInput(
          kmeansTaskGraph, plan, "kmeanssource", "points", dataPointsObject);
      taskExecutor.addInput(
          kmeansTaskGraph, plan, "kmeanssource", "centroids", centroidsDataObject);
      //actual execution of the third task graph
      taskExecutor.execute(kmeansTaskGraph, plan);
      //retrieve the new centroid value for the next iterations
      centroidsDataObject = taskExecutor.getOutput(kmeansTaskGraph, plan, "kmeanssink");
    }
```

#### New Centroid Updation
This process repeats for ‘n’ number of iterations as specified by the user. For every iteration, the 
new centroid value is calculated and the calculated value is distributed across all the task instances. 
At the end of every iteration, the centroid value is updated and the iteration continues with the new centroid value.

```java
    //retrieve the new centroid value for the next iterations
    centroidsDataObject = taskExecutor.getOutput(kmeansTaskGraph, plan, "kmeanssink");
```

### KMeansSourceTask 

First, the execute method in KMeansJobSource retrieve the partitioned data points into their respective task index values and the complete centroid values into their respective task index values. 
 
```java
    @Override
    public void execute() {
      int dim = Integer.parseInt(config.getStringValue("dim"));

      DataPartition<?> dataPartition = dataPointsObject.getPartitions(context.taskIndex());
      datapoints = (double[][]) dataPartition.getConsumer().next();

      DataPartition<?> centroidPartition = centroidsObject.getPartitions(context.taskIndex());
      centroid = (double[][]) centroidPartition.getConsumer().next();
```
The retrieved data points and centroids are sent to the KMeansCalculator to perform the actual distance calculation using the Euclidean distance. 

```java
      kMeansCalculator = new KMeansCalculator(datapoints, centroid, dim);
      double[][] kMeansCenters = kMeansCalculator.calculate();
```
      
Finally, each task instance write their calculated centroids value as given below:
```java
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

## Running K-Means Clustering
   
This command generate and write the datapoints and centroids in the local filesystem and run the 
K-Means clustering process. 
   
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerMain -dinput /tmp/dinput -cinput /tmp/cinput -fShared false -nFiles 1 -output /tmp/output -workers 2 -dim 2 -parallelism 4 -filesys local -dsize 1000 -csize 4 -iter 100
```
      
This command generate and write the datapoints and centroids in the HDFS and run the run the 
K-Means clustering process. 
   
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerMain -dinput hdfs://namenode:9000/tmp/dinput -cinput hdfs://namenode:9000/tmp/cinput -fShared false -nFiles 1 -output hdfs://namenode:9000/tmp/output -workers 2 -dim 2 -parallelism 4 -filesys hdfs -dsize 1000 -csize 4 -iter 10
```

### Sample Output 

```bash
[2019-03-25 15:27:01 -0400] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorker: 
Final Centroids After	100	iterations	[[0.2535406313735363, 0.25640515489554255], 
[0.7236140928643464, 0.7530306848028933], [0.7481226889281528, 0.24480221871888594], 
[0.2203460821168371, 0.754988220991043]]  

[2019-03-25 15:27:01 -0400] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorker: 
Final Centroids After	100	iterations	[[0.2535406313735363, 0.25640515489554255], 
[0.7236140928643464, 0.7530306848028933], [0.7481226889281528, 0.24480221871888594], 
[0.2203460821168371, 0.754988220991043]]  

[2019-03-25 15:27:01 -0400] [INFO] [worker-1] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker: 
Worker finished executing - 1  

[2019-03-25 15:27:01 -0400] [INFO] [worker-0] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker: 
Worker finished executing - 0  

[2019-03-25 15:27:01 -0400] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.JobMaster: All 2 workers have completed. 
JobMaster is stopping.  


```
