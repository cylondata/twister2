# Task Examples

Task examples demonstrate the task API. The task API has the capability of deploying
tasks by abstracting the communication logic to the API users. Basically, the task 
layer does the job of creating the task graph based on the user description and builds
the underlying communication. The task API also supports a thread model which user can
select based on preference. Task examples are in two forms representing batch and stream
mode examples. In these examples we address the collective communication usage with 
Twister2. 

## Twister2 Collective Communications

1. Reduce
2. Allreduce
3. Gather
4. AllGather
5. KeyedReduce
6. KeyedGather
7. Broadcast
8. Partition
After building the project, you can run the batch mode examples as follows. First you need
to extract the twister2-0.1.0-no-mpi.tar.gz located within the bazel-bin/scripts/package,
if you have already installed Open MPI separately and not using the project built Open MPI
version. 

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

First, you need to create a source and a sink task for the most simple data flow application
in Twister2. 

For the batch or stream tasks, the source task and sink task has to be defined. In the example
package we have abstracted the source task in the task abstraction. The source task means 
the task which generates the data or act as the entry point for the data flow 
to the programme. In the examples we generate an array of user given size in the 
source task.  


```java
BaseBatchSource g = new SourceBatchTask(edge);
BaseBatchSink r = new ReduceSinkTask();

```

```java
BaseStreamSource g = new SourceBatchTask(edge);
BaseStreamSink r = new ReduceSinkTask();

```

The BaseBatchSource and BaseBatchSink are the base level source and sink tasks for
batch mode applications. The BaseStreamSource and BaseStreamSink tasks appear as the
corresponding classes for stream mode applications.

For the source tasks we specify the edge name or the connection name between source
and the sink tasks. If there are multiple edges in a single application, the edge names
must be unique.

After creating the source and sink task, depending on the collective operation user must
create the task graph as stated in the following examples. The created source and 
sink tasks must be added to the task graph with source and sink names with unique identities.

The following [task graph](../../../concepts/task-system/task-graph/task-graph.md)  building refers to a reduction based example, which will be elaborated
in the following sections.  

```java
taskGraphBuilder.addSource(SOURCE, g, psource);
computeConnection = taskGraphBuilder.addSink(SINK, r, psink);
computeConnection.reduce(SOURCE, edge, operation, dataType);
```
  

## Batch Examples

### Reduce Example

For a reduce, allreduce or keyed-reduce, the operation parameter can be
sum, product or division.

```java  

@Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);

    String edge = "edge";
    BaseBatchSource g = new SourceBatchTask(edge);
    BaseBatchSink r = new ReduceSinkTask();

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.reduce(SOURCE, edge, Op.SUM, DataType.INTEGER);
    return taskGraphBuilder;
  }

```
In the buildTaskGraph method we specify the task graph that we are going to build.
This is a very simple task graph which has a source and a sink task with a user defined
parallelism.  The source task is abstracted from the user in the examples, but in the
BenchTaskWorker class, you can see the SourceBatchTask as the source task for all the
batch examples.

```java
protected static class SourceBatchTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public SourceBatchTask() {

    }

    public SourceBatchTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      Object val = generateData();
      int iterations = jobParameters.getIterations();
      while (count <= iterations) {
        if (count == iterations) {
          context.end(this.edge);
        } else if (count < iterations) {
          experimentData.setInput(val);
          if (context.write(this.edge, val)) {
            count++;
          }
        }
      }
    }
  }
```

Running a reduction operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "reduce" -stages 8,1 -verify

```
[Task based Batch Reduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTReduceExample.java)

### AllReduce Example

```java  

public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);

    String edge = "edge";
    BaseBatchSource g = new SourceBatchTask(edge);
    BaseBatchSink r = new AllReduceSinkTask();

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.allreduce(SOURCE, edge, Op.SUM, DataType.INTEGER);
    return taskGraphBuilder;
  }
```

Running a allreduce operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8, as this is all reduce task. The sink parallelism must be greater than
one, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "allreduce" -stages 8,8 -verify

```
[Task based Batch AllReduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTAllReduceExample.java)


### Gather Example

```java  

public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    BaseBatchSource g = new SourceBatchTask(edge);
    BaseBatchSink r = new GatherSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.gather(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }
```

Running a gather operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "gather" -stages 8,1 -verify

```
[Task based Batch Gather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTGatherExample.java)


### AllGather Example

```java  

 public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    BaseBatchSource g = new SourceBatchTask(edge);
    BaseBatchSink r = new AllGatherSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.allgather(SOURCE, edge, dataType);
    return taskGraphBuilder;
  }
```

Running a allgather operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "allgather" -stages 8,8 -verify

```
[Task based Batch AllGather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTAllGatherExample.java)

### Broadcast Example

```java  

 public TaskGraphBuilder buildTaskGraph() {
     List<Integer> taskStages = jobParameters.getTaskStages();
     int sourceParallelism = taskStages.get(0);
     int sinkParallelism = taskStages.get(1);
     String edge = "edge";
     BaseBatchSource g = new SourceBatchTask(edge);
     BaseBatchSink r = new BroadcastSinkTask();
     taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
     computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
     computeConnection.broadcast(SOURCE, edge);
     return taskGraphBuilder;
   }
```

Running a broadcast operation on a size of 8 array with 4 workers iterating once with source parallelism of 1
and sink parallelism of 8. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "broadcast" -stages 1,8 -verify

```
[Task based Batch Broadcast Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTBroadCastExample.java)


### Partition Example

```java  
 public TaskGraphBuilder buildTaskGraph() {
     List<Integer> taskStages = jobParameters.getTaskStages();
     int sourceParallelism = taskStages.get(0);
     int sinkParallelism = taskStages.get(1);
     DataType dataType = DataType.INTEGER;
     String edge = "edge";
     BaseBatchSource g = new SourceBatchTask(edge);
     BaseBatchSink r = new PartitionSinkTask();
     taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
     computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
     computeConnection.partition(SOURCE, edge, dataType);
     return taskGraphBuilder;
   }
```

Running a partition operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "partition" -stages 8,8 -verify

```
[Task based Batch Partition Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTPartitionExample.java)


### KeyedReduce Example

```java  
  public TaskGraphBuilder buildTaskGraph() {
      List<Integer> taskStages = jobParameters.getTaskStages();
      int sourceParallelsim = taskStages.get(0);
      int sinkParallelism = taskStages.get(1);
      Op operation = Op.SUM;
      DataType keyType = DataType.OBJECT;
      DataType dataType = DataType.INTEGER;
      String edge = "edge";
      BaseBatchSource g = new SourceBatchTask(edge);
      BaseBatchSink r = new KeyedReduceSinkTask();
      taskGraphBuilder.addSource(SOURCE, g, sourceParallelsim);
      computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
      computeConnection.keyedReduce(SOURCE, edge, operation, keyType, dataType);
      return taskGraphBuilder;
    }
```

Running a keyed reduce operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "keyed-reduce" -stages 8,1 -verify

```
[Task based Batch Keyed-Reduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTKeyedReduceExample.java)



### KeyedGather Example

```java  
  public TaskGraphBuilder buildTaskGraph() {
      List<Integer> taskStages = jobParameters.getTaskStages();
      int sourceParallelism = taskStages.get(0);
      int sinkParallelism = taskStages.get(1);
      DataType keyType = DataType.OBJECT;
      DataType dataType = DataType.INTEGER;
      String edge = "edge";
      BaseBatchSource g = new SourceBatchTask(edge);
      BaseBatchSink r = new KeyedGatherSinkTask();
      taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
      computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
      computeConnection.keyedGather(SOURCE, edge, keyType, dataType);
      return taskGraphBuilder;
    }
```

Running a keyed gather operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "keyed-gather" -stages 8,1 -verify

```
[Task based Batch Keyed-Gather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTKeyedGatherExample.java)



### KeyedPartition Example

```java  
   public TaskGraphBuilder buildTaskGraph() {
      List<Integer> taskStages = jobParameters.getTaskStages();
      int sourceParallelism = taskStages.get(0);
      int sinkParallelism = taskStages.get(1);
      DataType dataType = DataType.INTEGER;
      DataType keyType = DataType.INTEGER;
      String edge = "edge";
      BaseBatchSource g = new KeyedSourceBatchTask(edge);
      BaseBatchSink r = new KeyedPartitionSinkTask();
      taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
      computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
      computeConnection.keyedPartition(SOURCE, edge, keyType, dataType);
      return taskGraphBuilder;      
    }
```

Running a keyed partition operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "keyed-partition" -stages 8,8 -verify

```
[Task based Batch Keyed-Partition Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTKeyedPartitionExample.java)


## Streaming Examples

To run the stream examples for each of the collective communication model, please use the -stream
tag. 

#### Example

Running a reduce streaming example using 4 workers, with a single iteration, source parallelism
as 8 and sink parallelism as one. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "reduce" -stages 8,1 -verify -stream

```

You can use the same running commands used for batch examples, but use the -stream tag.






