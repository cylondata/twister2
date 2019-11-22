---
id: connecteddataflow 
title: ConnectedDataflow
sidebar_label: connecteddataflow
---

## Connected Dataflow Graph
The Connected DataFlow graph is to compose multiple independent/dependent dataflow task graphs into 
a single entity. A dataflow task graph consists of multiple subtasks which are arranged based on the 
parent-child relationship between the tasks. In general, a dataflow task graph consists of multiple 
task vertices and edges to connect those vertices. The vertices represent the characteristics of 
computations and edges represent the communication between those computations. 

#### Connected Dataflow Based K-Means Clustering Implementation

The implementation details of k-means clustering using Connected Dataflow in Twister2 is discussed
below.

### CDFConstants

The constants which are used by the k-means algorithm to specify the number of workers, parallelism, 
dimension, size of datapoints, size of centroids, file system, number of iterations, datapoints, and 
centroids directory. 

```java
  public static final String ARGS_WORKERS = "workers";
  public static final String ARGS_PARALLELISM_VALUE = "parallelism";
  public static final String ARGS_DIMENSIONS = "dim";
  public static final String ARGS_ITERATIONS = "iter";
  public static final String ARGS_DSIZE = "dsize";
  public static final String ARGS_CSIZE = "csize";
  public static final String ARGS_DINPUT = "dinput";
  public static final String ARGS_CINPUT = "cinput";
```

### K-Means Connected Dataflow Driver

The KMeansDriver is the driver program for the k-means connected dataflow example which extends the 
BaseDriver class. The execute() method in the driver program call the respective dataflow task graphs
to generate the datapoints and centroids, process the datapoints and centroids, and invoke the task
graph to perform the clustering process. The driver calls the clustering task graph (in this example
fourth task graph) for 'n' number of iterations. 

```java
public static class KMeansDriver extends BaseDriver {

    @Override
    public void execute(CDFWEnv cdfwEnv) {
      Config config = cdfwEnv.getConfig();
      DataFlowJobConfig jobConfig = new DataFlowJobConfig();

      String dataDirectory = String.valueOf(config.get(CDFConstants.ARGS_DINPUT));
      String centroidDirectory = String.valueOf(config.get(CDFConstants.ARGS_CINPUT));
      int parallelism =
          Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_PARALLELISM_VALUE)));
      int instances = Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_WORKERS)));
      int iterations =
          Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_ITERATIONS)));
      int dimension = Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_DIMENSIONS)));
      int dsize = Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_DSIZE)));
      int csize = Integer.parseInt(String.valueOf(config.get(CDFConstants.ARGS_CSIZE)));

      DataFlowGraph job = generateData(config, dataDirectory, centroidDirectory, dimension,
          dsize, csize, instances, parallelism, jobConfig);
      cdfwEnv.executeDataFlowGraph(job);

      DataFlowGraph job1 = generateFirstJob(config, parallelism, dataDirectory, dimension,
          dsize, instances, jobConfig);
      DataFlowGraph job2 = generateSecondJob(config, parallelism, centroidDirectory, dimension,
          csize, instances, jobConfig);

      long startTime = System.currentTimeMillis();
      cdfwEnv.executeDataFlowGraph(job1);
      cdfwEnv.executeDataFlowGraph(job2);
      long endTimeData = System.currentTimeMillis();

      for (int i = 0; i < iterations; i++) {
        DataFlowGraph job3 = generateThirdJob(config, parallelism, instances, iterations,
            dimension, jobConfig);
        job3.setIterationNumber(i);
        cdfwEnv.executeDataFlowGraph(job3);
      }
      long endTime = System.currentTimeMillis();
      LOG.info("Total K-Means Execution Time: " + (endTime - startTime)
          + "\tData Load time : " + (endTimeData - startTime)
          + "\tCompute Time : " + (endTime - endTimeData)); 
      cdfwEnv.close();
    }
  }
```

### Datapoints Generation
This  task graph is responsible for generating the datapoints and centroids for the k-means 
clustering process. The user can specify either the local filesystem or the HDFS directory to 
generate the input files required for the k-means clustering.

```java
  DataGeneratorSource dataGeneratorSource = new DataGeneratorSource(Context.TWISTER2_DIRECT_EDGE,
        dsize, csize, dimension, dataDirectory, centroidDirectory);
    DataGeneratorSink dataGeneratorSink = new DataGeneratorSink();
    ComputeGraphBuilder dataGenerationGraphBuilder = ComputeGraphBuilder.newBuilder(config);
    dataGenerationGraphBuilder.setTaskGraphName("DataGenerator");
    dataGenerationGraphBuilder.addSource("datageneratorsource", dataGeneratorSource, parallel);
```

Then, add this graph as a sub graph for the connected dataflow graph and set the worker instances 
and the graph type. 

```java
    DataFlowGraph job = DataFlowGraph.newSubGraphJob("datageneratorTG", dataObjectTaskGraph)
        .setWorkers(workers).addDataFlowJobConfig(jobConfig)
        .setGraphType("non-iterative");
```

### Reading and partitioning the Datapoints

The main functionality of this task graph is to partition the data points, convert the 
partitioned datapoints into two-dimensional array, and write the two-dimensional array into their 
respective task index values. 

```java
    /*Graph to partition and read the partitioned data points **/
    DataObjectSource dataObjectSource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    KMeansDataObjectCompute dataObjectCompute = new KMeansDataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue, dimension);
    KMeansDataObjectDirectSink dataObjectSink = new KMeansDataObjectDirectSink("points");
    ComputeGraphBuilder datapointsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);
```

First, add the source, compute, and sink tasks to the task graph builder for the task graph. 
Then, create the communication edges between the tasks for the first task graph.
 
```java
    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsComputeGraphBuilder.addSource("datapointsource", dataObjectSource,
        parallelismValue);
    ComputeConnection datapointComputeConnection = datapointsComputeGraphBuilder.addCompute(
        "datapointcompute", dataObjectCompute, parallelismValue);
    ComputeConnection firstGraphComputeConnection = datapointsComputeGraphBuilder.addCompute(
        "datapointsink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    datapointComputeConnection.direct("datapointsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    firstGraphComputeConnection.direct("datapointcompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsComputeGraphBuilder.setMode(OperationMode.BATCH);

    datapointsComputeGraphBuilder.setTaskGraphName("datapointsTG");
    ComputeGraph firstGraph = datapointsComputeGraphBuilder.build();
```

Then, add this graph as a sub graph for the connected dataflow graph and set the worker instances and
the graph type. 

```java
    DataFlowGraph job = DataFlowGraph.newSubGraphJob("datapointsTG", firstGraph)
            .setWorkers(instances).addDataFlowJobConfig(jobConfig)
            .setGraphType("non-iterative");
```

#### DataObjectSource
This class partition the datapoints which is based on the task parallelism value. It may use 
either the "LocalTextInputPartitioner" or "LocalFixedInputPartitioner" to partition the datapoints. 
Finally, write the partitioned datapoints into their respective edges. The LocalTextInputPartitioner
partition the datapoints based on the block whereas the LocalFixedInputPartitioner partition the 
datapoints based on the length of the file. For example, if the task parallelism is 4, if there are 
16 data points and each task will get 4 datapoints to process. 

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

### Reading and partitioning the Centroids 

Similar to the datapoints, the second task graph performs three processes namely partitioning, 
converting the partitioned centroids into array, and writing into respective task index values 
but, with one major difference of read the complete file as one partition. 

 1. DataFileReplicatedReadSource
 2. KMeansDataObjectCompute
 3. KMeansDataObjectDirectSink 
 
```java
   DataFileReplicatedReadSource dataFileReplicatedReadSource
        = new DataFileReplicatedReadSource(Context.TWISTER2_DIRECT_EDGE, centroidDirectory);
    KMeansDataObjectCompute centroidObjectCompute = new KMeansDataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, csize, dimension);
    KMeansDataObjectDirectSink centroidObjectSink = new KMeansDataObjectDirectSink("centroids");
    ComputeGraphBuilder centroidsComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);;
```

Similar to the previous task graph, it add the source, compute, and sink tasks to the task graph 
builder for the second task graph. Then, create the communication edges between the tasks for the 
second task graph.

```java
    //Add source, compute, and sink tasks to the task graph builder for the second task graph
    centroidsComputeGraphBuilder.addSource("centroidsource", dataFileReplicatedReadSource,
        parallelismValue);
    ComputeConnection centroidComputeConnection = centroidsComputeGraphBuilder.addCompute(
        "centroidcompute", centroidObjectCompute, parallelismValue);
    ComputeConnection secondGraphComputeConnection = centroidsComputeGraphBuilder.addCompute(
        "centroidsink", centroidObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    centroidComputeConnection.direct("centroidsource")
            .viaEdge(Context.TWISTER2_DIRECT_EDGE)
            .withDataType(MessageTypes.OBJECT);
        secondGraphComputeConnection.direct("centroidcompute")
            .viaEdge(Context.TWISTER2_DIRECT_EDGE)
            .withDataType(MessageTypes.OBJECT);
        centroidsComputeGraphBuilder.setMode(OperationMode.BATCH);
        centroidsComputeGraphBuilder.setTaskGraphName("centroidTG");
     ComputeGraph secondGraph = centroidsComputeGraphBuilder.build();
```

Then, add this graph as a sub graph for the connected dataflow graph and set the worker instances 
and the graph type. 

```java
   DataFlowGraph job = DataFlowGraph.newSubGraphJob("centroidTG", secondGraph)
            .setWorkers(instances).addDataFlowJobConfig(jobConfig)
            .setGraphType("non-iterative");
```

#### DataFileReplicatedReadSource

This class uses the "LocalCompleteTextInputPartitioner" to read the whole file from the centroids 
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

### K-Means Clustering 

This task graph has the following classes namely KMeansSource, KMeansAllReduceTask, and 
CentroidAggregator. Similar to the first and second task graph, first we have to add the source, 
sink, and communication edges to the third task graph. 

```java
    KMeansSourceTask kMeansSourceTask = new KMeansSourceTask(dimension);
    KMeansAllReduceTask kMeansAllReduceTask = new KMeansAllReduceTask();
    ComputeGraphBuilder kmeansComputeGraphBuilder = ComputeGraphBuilder.newBuilder(config);

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
    ComputeGraph thirdGraph = kmeansComputeGraphBuilder.build();
```


Then, add this graph as a sub graph for the connected dataflow graph and set the worker instances and
the graph type. 

```java
DataFlowGraph job = DataFlowGraph.newSubGraphJob("kmeansTG", thirdGraph)
        .setWorkers(instances).addDataFlowJobConfig(jobConfig)
        .setGraphType("iterative")
        .setIterations(iterations);
```

## To Run Connected Dataflow Based K-Means Clustering

This command generate and write the datapoints and centroids in the local filesystem and run the 
K-Means clustering process. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.cdfw.KMeansConnectedDataflowExample -workers 2 -parallelism 4 -dim 2 -dsize 10000 -csize 4 -dinput /tmp/dinput -cinput /tmp/cinput -iter 10
```

## Sample Output

```text
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: datageneratorTG  
[2019-11-22 14:44:24 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.task.impl.cdfw.CDFWRuntime: 2 workers joined.   
[2019-11-22 14:44:24 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.task.impl.cdfw.CDFWRuntime: 2 workers joined.   
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: datapointsTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: centroidTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: kmeansTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: kmeansTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: kmeansTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: kmeansTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: kmeansTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: kmeansTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: kmeansTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: kmeansTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: kmeansTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.task.cdfw.CDFWExecutor: Sending graph to workers for execution: kmeansTG  
[2019-11-22 14:44:24 -0500] [INFO] [-] [driver] edu.iu.dsc.tws.examples.batch.cdfw.KMeansConnectedDataflowExample: Total K-Means Execution Time: 384	Data Load time : 189	Compute Time : 195  
[2019-11-22 14:44:25 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.task.impl.cdfw.CDFWRuntime: 0Received CDFW job completed message. Leaving execution loop  
[2019-11-22 14:44:25 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.task.impl.cdfw.CDFWRuntime: 0 Execution Completed  
[2019-11-22 14:44:25 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker: Worker finished executing - 0  
[2019-11-22 14:44:25 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.task.impl.cdfw.CDFWRuntime: 1Received CDFW job completed message. Leaving execution loop  
[2019-11-22 14:44:25 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.task.impl.cdfw.CDFWRuntime: 1 Execution Completed  
[2019-11-22 14:44:25 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker: Worker finished executing - 1  
[2019-11-22 14:44:25 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerMonitor: Worker:1 COMPLETED.  
[2019-11-22 14:44:25 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerMonitor: Worker:0 COMPLETED.  
[2019-11-22 14:44:25 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerMonitor: All 2 workers COMPLETED. Terminating the job.  
[2019-11-22 14:44:25 -0500] [INFO] [-] [main] edu.iu.dsc.tws.rsched.core.ResourceAllocator: CLEANED TEMPORARY DIRECTORY......:/tmp/twister2-kmeans-connected-dataflow-4803835711749628541  
```
