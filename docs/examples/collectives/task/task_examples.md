# Task Examples

Task examples demonstrate the task API which has the capability of deploying tasks by abstracting 
the communication logic to the API users. Basically, the task layer does the job of creating the 
task graph based on the user description and builds the underlying communication. The task API 
also supports a thread model which user can select based on preference. Task examples are in two 
forms representing batch and stream mode examples. In these examples we address the collective 
communication usage with Twister2. 

## Twister2 Collective Communications

1. Reduce
2. Allreduce
3. Gather
4. AllGather
5. KeyedReduce
6. KeyedGather
7. Broadcast
8. Partition
9. KeyedPartition

After building the project, you can run the batch mode examples as follows. First you need to extract 
the twister2-0.2.1.tar.gz located within the bazel-bin/scripts/package, if you have already installed 
Open MPI separately and not using the project built Open MPI version. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr <iterations> -workers <workers> -size <data_size> -op "<operation>" -stages <source_parallelsim>,<sink_parallelism> -<flag> -verify
```

### Running Option Definitions

1. itr : integer number which provides the number of iterations which the application must run
2. workers : Number of workers needed for the application
3. data_size : size of the array that is being passed. In the examples, we generate an array specified by this size
4. op : Collective operation type:
    1. reduce
    2. allreduce
    3. gather
    4. allgather
    5. keyed-reduce
    6. keyed-gather
    7. bcast
    8. partition
    9. keyed-partition
5. stages : has to params 
    1. source_parallelism : depending on the collective communication that you prefer select this number
    2. sink_parallelism : depending on the collective communication that you prefer select this number
    Example : For reduce communication it can be 4,1 as sink has to be a single point of receive in the reduction 
    process and source has more data generating points. 
6. flag : for a batch job this flag must be empty, if it is an streaming job, -stream would state the flag. 
7. verify : -verify flag does the verification of the collective operation. This is an optional parameter. 

First, you need to create a source and a sink task for the most simple data flow application in Twister2. 

For the batch or stream tasks, the source task and sink task has to be defined. In the example
package we have abstracted the source task in the task abstraction. The source task means 
the task which generates the data or act as the entry point for the data flow to the programmer. 
In the examples we generate an array of user given size in the source task.  

```java 
BaseSource g = new SourceTask(edge);
ISink r = new ReduceSinkTask();
```

The BaseSource and ISink are the base level source and sink tasks for batch and streaming mode 
applications.

For the source tasks we specify the edge name or the connection name between source and the sink tasks. 
If there are multiple edges in a single application, the edge names must be unique.

After creating the source and sink task, depending on the collective operation user must create the 
task graph as stated in the following examples. The created source and sink tasks must be added to 
the task graph with source and sink names with unique identities.

The following [task graph](../../../concepts/task-system/task-graph/task-graph.md)  building refers 
to a reduction based example, which will be elaborated in the following sections.  

```java 
taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
computeConnection.reduce(SOURCE, edge, operation, DataType.INTEGER);
```

### AbstractSingleDataCompute and AbstractIterableDataCompute

The `AbstractSingleDataCompute<T>` and `AbstractIterableDataCompute<Iterator<T>>` are the two abstract 
classes abstracted from the base class of `BaseCompute<T>` and `BaseCompute<Iterator<T>>` which is mainly
used to define the type of data coming from the communication operations either single or an
or an iterator.
   
### Batch Task Graph Examples

The following are the examples of batch processing.

1. BTReduceExample
2. BTAllGatherExample
3. BTAllReduceExample
4. BTBroadCastExample
5. BTGatherExample
6. BTKeyedGatherExample
7. BTKeyedReduceExample
8. BTPartitionExample
9. BTPartitionKeyedExample 

Each example explains the batch operation with the respective dataflow communication operations 
namely allgather, allreduce, broadcast, gather, keyedgather, keyedreduce, partition, keyedpartition, 
and reduce.

### Batch Reduce Example

For a reduce, allreduce or keyed-reduce, the operation parameter can be sum, product or division.

In the buildTaskGraph method we specify the task graph that we are going to build. This is a very 
simple task graph which has a source and a sink task with a user defined parallelism. The source 
task is abstracted from the user in the examples, but in the BenchTaskWorker class, you can see the 
SourceTask as the source task for all the batch examples.

```java  
@Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);

    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ISink r = new ReduceSinkTask();

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.reduce(SOURCE, edge, Op.SUM, DataType.INTEGER);
    return taskGraphBuilder;
  }
```
  
The sink task is abstracted from `ReduceCompute<int[]>` and the `ReduceCompute<T>` is abstracted from 
`AbstractSingleDataCompute<T>`.

```java 
@SuppressWarnings({"rawtypes", "unchecked"})
  protected static class ReduceSinkTask extends ReduceCompute<int[]> implements ISink {
    private static final long serialVersionUID = -254264903510284798L;

    private boolean timingCondition;

    private ResultsVerifier<int[], int[]> resultsVerifier;
    private boolean verified = true;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.timingCondition = getTimingCondition(SINK, context);
      resultsVerifier = new ReduceVerifier(inputDataArray, ctx, SOURCE, jobParameters);
    }

    @Override
    public boolean reduce(int[] content) {
      Timing.mark(BenchmarkConstants.TIMING_ALL_RECV, this.timingCondition);
      LOG.info(String.format("%d received reduce %d", context.getWorkerId(), context.taskId()));
      BenchmarkUtils.markTotalTime(resultsRecorder, this.timingCondition);
      resultsRecorder.writeToCSV();
      this.verified = verifyResults(resultsVerifier, content, null, verified);
      return true;
    }
```

[Task based Batch Reduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTReduceExample.java)

### To Run Batch Reduce Example

Running a reduction operation on a size of 8 array with 4 workers iterating once with source parallelism 
of 8 and sink parallelism of 1, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "reduce" -stages 8,1 -verify
```

### Batch AllReduce Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the batch examples. 

```java 
@Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);

    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ISink r = new AllReduceSinkTask();

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.allreduce(SOURCE, edge, Op.SUM, DataType.INTEGER);
    return taskGraphBuilder;
  }
```
The sink task is abstracted from `AllReduceCompute<int[]>` and the `AllReduceCompute<T>` is further 
abstracted from `AbstractSingleDataCompute<T>`.

[Task based Batch AllReduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTAllReduceExample.java)


### To Run Batch AllReduce Example

Running an allreduce operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8, as this is all reduce task. The sink parallelism must be greater than
one, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "allreduce" -stages 8,8 -verify
```
### Batch Gather Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the batch examples. 

```java 
@Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    
    BaseSource g = new SourceTask(edge);
    ISink r = new GatherSinkTask();
    
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.gather(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }
```
The sink task is abstracted from `GatherCompute<int[]>` and the `GatherCompute<T>` is further abstracted 
from `AbstractIterableDataCompute<Tuple<Integer, T>>`.

[Task based Batch Gather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTGatherExample.java)

### To Run Batch Gather Example

Running a gather operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "gather" -stages 8,1 -verify
```

### Batch AllGather Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the batch examples. 

```java 
 @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    
    String edge = "edge";
    ISource g = new SourceTask(edge);
    ISink r = new AllGatherSinkTask();
    
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.allgather(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }
```
The sink task is abstracted from `AllGatherCompute<int[]>` and the `AllGatherCompute<T>` is further abstracted 
from  `AbstractIterableDataCompute<Tuple<Integer, T>>`.

[Task based Batch AllGather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTAllGatherExample.java)

### To Run Batch AllGather Example

Running a allgather operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "allgather" -stages 8,8 -verify
```

### Batch Broadcast Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the batch examples. 

```java 
  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    
    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ISink r = new BroadcastSinkTask();
    
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.broadcast(SOURCE, edge);
    return taskGraphBuilder;
  }
```
The sink task is abstracted from `BBroadCastCompute<int[]>`  and the `BBroadCastCompute<T>` is further 
abstracted from `AbstractIterableDataCompute<T>`.

[Task based Batch Broadcast Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTBroadCastExample.java)

### To Run Batch Broadcast Example

Running a broadcast operation on a size of 8 array with 4 workers iterating once with source parallelism of 1
and sink parallelism of 8. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "bcast" -stages 1,8 -verify
```

### Batch Partition Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the batch examples. 

```java 
 @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    
    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ISink r = new PartitionSinkTask();
    
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.partition(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }
```

The sink task is abstracted from `BPartitionCompute<int[]>` and the `BPartitionCompute<T>` is further 
abstracted from `AbstractIterableDataCompute<T>`.

[Task based Batch Partition Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTPartitionExample.java)

### To Run Batch Partition Example

Running a partition operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "partition" -stages 8,8 -verify
```

### Batch KeyedReduce Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the batch examples. 

```java 
@Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelsim = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    Op operation = Op.SUM;
    DataType keyType = DataType.INTEGER;
    DataType dataType = DataType.INTEGER;
    
    String edge = "edge";
    BaseSource g = new SourceTask(edge, true);
    ISink r = new KeyedReduceSinkTask();
    
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelsim);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedReduce(SOURCE, edge, operation, keyType, dataType);
    return taskGraphBuilder;
  }
```
The sink task is abstracted from `BKeyedReduceCompute<Integer, int[]>` and the `BKeyedReduceCompute<K, T>` 
is further abstracted from `AbstractIterableDataCompute<Tuple<K, T>>`.

[Task based Batch Keyed-Reduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTKeyedReduceExample.java)

### To Run Batch KeyedReduce Example

Running a keyed reduce operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "keyed-reduce" -stages 8,1 -verify
```

### Batch KeyedGather Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the batch examples. 

```java 
 @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType keyType = DataType.INTEGER;
    DataType dataType = DataType.INTEGER;
    
    String edge = "edge";
    BaseSource g = new SourceTask(edge, true);
    ISink r = new KeyedGatherSinkTask();
    
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedGather(SOURCE, edge, keyType, dataType);
    return taskGraphBuilder;
  }
```

The sink task is abstracted from `BKeyedGatherCompute<Integer, int[]>` and the `BKeyedGatherCompute<K, T>` 
is further abstracted from `AbstractIterableDataCompute<Tuple<K, Iterator<T>>>`.

[Task based Batch Keyed-Gather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTKeyedGatherExample.java)


### To Run Batch KeyedGather Example

Running a keyed gather operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "keyed-gather" -stages 8,1 -verify
```

### Batch KeyedPartition Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the batch examples. 

```java 
 @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    DataType keyType = DataType.INTEGER;
    
    String edge = "edge";
    BaseSource g = new SourceTask(edge, true);
    ISink r = new BKeyedPartitionSinkTask();
    
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedPartition(SOURCE, edge, keyType, dataType);
    return taskGraphBuilder;
  }
```

The sink task is abstracted from `BPartitionKeyedCompute<Integer, int[]>` and the `BPartitionKeyedCompute<K, T>` 
is further abstracted from `AbstractIterableDataCompute<Tuple<K, T>>`.

[Task based Batch Keyed-Partition Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTKeyedPartitionExample.java)


### To Run Batch KeyedPartition Example

Running a keyed partition operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8. 
  
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "keyed-partition" -stages 8,8 -verify
```

### Streaming Task Graph Examples

To run the stream examples for each of the collective communication model, please use the -stream
tag. The following are the examples of streaming processing.

1. STAllGatherExample
2. STAllReduceExample
3. STBroadCastExample
4. STGatherExample
5. STPartitionExample
6. STPartitionKeyedExample
7. STReduceExample
8. STKeyedGatherExample 
9. STKeyedReduceExample

Each example explains the streaming operation with the respective dataflow communication operations 
namely gather, reduce, broadcast, keyedgather, keyedreduce, partition, keyedpartition, and allreduce.

### Streaming Reduce Example

For a reduce, allreduce or keyed-reduce, the operation parameter can be sum, product or division. 

Similar to the batch task graph, in the buildTaskGraph method we specify the task graph that we are 
going to build. This is a very simple task graph which has a source and a sink task with a userdefined 
parallelism. The source task is abstracted from the user in the examples, but in the BenchTaskWorker
class, you can see the SourceTask as the source task for all the streaming examples. 

```java 
@Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);

    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ISink r = new ReduceSinkTask();

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.reduce(SOURCE, edge, Op.SUM, DataType.INTEGER);

    return taskGraphBuilder;
  }
```

The sink task is abstracted from `ReduceCompute<int[]>` and the `ReduceCompute<T>` is abstracted from 
`AbstractSingleDataCompute<T>`.

```java 
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected static class ReduceSinkTask extends ReduceCompute<int[]> implements ISink {
      private static final long serialVersionUID = -254264903510284798L;
      private ResultsVerifier<int[], int[]> resultsVerifier;
      private boolean verified = true;
      private boolean timingCondition;
  
      private int count = 0;
  
      @Override
      public void prepare(Config cfg, TaskContext ctx) {
        super.prepare(cfg, ctx);
        this.timingCondition = getTimingCondition(SINK, context);
        resultsVerifier = new ReduceVerifier(inputDataArray, ctx, SOURCE);
        receiversInProgress.incrementAndGet();
      }
  
      @Override
      public boolean reduce(int[] data) {
        count++;
        if (count > jobParameters.getWarmupIterations()) {
          Timing.mark(BenchmarkConstants.TIMING_MESSAGE_RECV, this.timingCondition);
        }
  
        if (count == jobParameters.getTotalIterations()) {
          LOG.info(String.format("%d received all-reduce %d",
              context.getWorkerId(), context.taskId()));
          Timing.mark(BenchmarkConstants.TIMING_ALL_RECV, this.timingCondition);
          BenchmarkUtils.markTotalAndAverageTime(resultsRecorder, this.timingCondition);
          resultsRecorder.writeToCSV();
          receiversInProgress.decrementAndGet();
        }
        this.verified = verifyResults(resultsVerifier, data, null, verified);
        return true;
      }
    }
```

[Task based Streaming Reduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/streaming/STReduceExample.java)

### To Run Streaming Reduce Example

Running a reduce streaming example using 4 workers, with a single iteration, source parallelism
as 8 and sink parallelism as one. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "reduce" -stages 8,1 -verify -stream
```

### Streaming AllReduce Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the streaming examples. 

```java 
 @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);

    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ISink r = new AllReduceSinkTask();

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.allreduce(SOURCE, edge, Op.SUM, DataType.INTEGER);

    return taskGraphBuilder;
  }
```
The sink task is  abstracted from `AllReduceCompute<int[]>` and the `AllReduceCompute<T>` is abstracted from 
`AbstractSingleDataCompute<T>`.

[Task based Streaming AllReduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/streaming/STAllReduceExample.java)

### To Run Streaming AllReduce Example

Running an allreduce operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8, as this is all reduce task. The sink parallelism must be greater than
one, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "allreduce" -stages 8,8 -verify -stream
```

### Streaming Gather Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the streaming examples.

```java 
@Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    
    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ISink r = new GatherSinkTask();
    
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.gather(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }
```

The sink task is abstracted from `GatherCompute<int[]>` and the `GatherCompute<T>` is abstracted from 
`AbstractIterableDataCompute<Tuple<Integer, T>>`.

[Task based Streaming Gather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/streaming/STGatherExample.java)

### To Run Streaming Gather Example

Running a gather operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "gather" -stages 8,1 -verify -stream
```

### Streaming AllGather Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the streaming examples. 

```java 
   @Override
    public TaskGraphBuilder buildTaskGraph() {
      List<Integer> taskStages = jobParameters.getTaskStages();
      int psource = taskStages.get(0);
      int psink = taskStages.get(1);
      DataType dataType = DataType.INTEGER;
      
      String edge = "edge";
      BaseSource g = new SourceTask(edge);
      ISink r = new AllGatherSinkTask();
      
      taskGraphBuilder.addSource(SOURCE, g, psource);
      computeConnection = taskGraphBuilder.addSink(SINK, r, psink);
      computeConnection.allgather(SOURCE, edge, dataType);
      return taskGraphBuilder;
    }
```

The sink task is abstracted from `AllGatherCompute<int[]>` and the `AllGatherCompute<T>` is abstracted from 
`AbstractIterableDataCompute<Tuple<Integer, T>>`.

[Task based Streaming AllGather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/streaming/STAllGatherExample.java)

### To Run Streaming AllGather Example

Running an allgather operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "allgather" -stages 8,8 -verify -stream
```

### Streaming Broadcast Example
The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the streaming examples. 

```java 
 @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);

    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ISink r = new BroadCastSinkTask();

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.broadcast(SOURCE, edge);
    return taskGraphBuilder;
  }
```
The sink task is  abstracted from `SBroadCastCompute<int[]>` and the `SBroadCastCompute<T>` is abstracted from 
`AbstractSingleDataCompute<T>`.

[Task based Streaming Broadcast Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/streaming/STBroadCastExample.java)

### To Run Streaming Broadcast Example

Running a broadcast operation on a size of 8 array with 4 workers iterating once with source parallelism of 1
and sink parallelism of 8. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "bcast" -stages 1,8 -verify -stream
```

### Streaming Partition Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the streaming examples. 

```java 
@Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    
    String edge = "edge";
    BaseSource g = new SourceTask(edge);
    ISink r = new PartitionSinkTask();
    
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.partition(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }
```
The sink task is abstracted from `SPartitionCompute<int[]>` and the `SPartitionCompute<T>` is abstracted from 
`AbstractSingleDataCompute<T>`.

[Task based Streaming Partition Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/streaming/STPartitionExample.java)

### To Run Streaming Partition Example

Running a partition operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "partition" -stages 8,8 -verify -stream
```

### Streaming KeyedPartition Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceTask as the source task for all the streaming examples. 

```java 
@Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    
    DataType keyType = DataType.INTEGER;
    DataType dataType = DataType.INTEGER;
    
    String edge = "edge";
    BaseSource g = new SourceTask(edge, true);
    ISink r = new SKeyedPartitionSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedPartition(SOURCE, edge, keyType, dataType);
    return taskGraphBuilder;
  }
```

The sink task is abstracted from `SPartitionKeyedCompute<Integer, int[]>` and the `SPartitionKeyedCompute<K, T>` 
is abstracted from `AbstractSingleDataCompute<Tuple<K, T>>`.

[Task based Batch Keyed-Partition Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/streaming/STPartitionKeyedExample.java)


### To Run Streaming KeyedPartition Example

Running a keyed partition operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "keyed-partition" -stages 8,8 -verify -stream
```

### Streaming KeyedReduce Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceStreamTask as the source task for all the streaming examples. 

```java 
@Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    Op operation = Op.SUM;
    DataType keyType = DataType.OBJECT;
    DataType dataType = DataType.INTEGER;
    
    String edge = "edge";
    BaseSource g = new KeyedSourceStreamTask(edge);
    ISink r = new KeyedReduceSinkTask();
    
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedReduce(SOURCE, edge, operation, keyType, dataType);
    return taskGraphBuilder;
  }
```
The sink task is  abstracted from `SKeyedReduceCompute<Object, int[]>` and the `SKeyedReduceCompute<K, T>` 
is abstracted from `AbstractSingleDataCompute<Tuple<K, T>>`.

[Task based Streaming Keyed-Reduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/streaming/STKeyedReduceExample.java)

### To Run Streaming KeyedReduce Example

Running a keyed reduce operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "keyed-reduce" -stages 8,1 -verify -stream
```

### Streaming KeyedGather Example

The source task is abstracted from the user in the examples, but in the BenchTaskWorker class, you 
can see the SourceStreamTask as the source task for all the streaming examples. 

```java 
@Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType keyType = DataType.OBJECT;
    DataType dataType = DataType.INTEGER;
    
    String edge = "edge";
    BaseSource g = new KeyedSourceStreamTask(edge);
    ISink r = new KeyedGatherSinkTask();
    
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedGather(SOURCE, edge, keyType, dataType);
    return taskGraphBuilder;
  }
```

The sink task is abstracted from `KeyedGatherCompute<Object, int[]>` and the `KeyedGatherCompute<K, T>` 
is abstracted from `AbstractIterableDataCompute<Tuple<K, T>>`.

[Task based Streaming Keyed-Gather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/streaming/STKeyedGatherExample.java)


### To Run Streaming KeyedGather Example

Running a keyed gather operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "keyed-gather" -stages 8,1 -verify -stream
```
