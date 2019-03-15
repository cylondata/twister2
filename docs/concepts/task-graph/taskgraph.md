## Task Graph

The task graph is the preferred choice for the processing of large-scale data. 
It simplifies the process of task parallelism and has the ability to dynamically determine the 
dependency between those tasks. The nodes in the task graph consist of task vertices and 
edges in which task vertices represent the computational units of an application and edges represent
the communication edges between those computational units. In other words, it describes the details 
about how the data is consumed between those units. Each node in the task graph holds the 
information about the input and its output. The task graph is converted into an execution graph 
once the actual execution takes place.

## Streaming Task Graph

Stream refers the process of handling unbounded sequence of data units. The streaming application 
that can continuosly consumes input stream units and produces the output stream units. The streaming
task graph is mainly responsible for building and executing the streaming applications.

### Streaming Task Graph Creation

This is the code snippet for creating the streaming task graph creation with reduce dataflow 
communication operation.

```text
   @Override
    public TaskGraphBuilder buildTaskGraph() {
      List<Integer> taskStages = jobParameters.getTaskStages();
      int sourceParallelism = taskStages.get(0);
      int sinkParallelism = taskStages.get(1);
  
      String edge = "edge";
      BaseSource g = new SourceStreamTask(edge);
      BaseSink r = new ReduceSinkTask();
  
      taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
      computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
      computeConnection.reduce(SOURCE, edge, Op.SUM, DataType.INTEGER);
  
      return taskGraphBuilder;
    }
```

### Streaming Task Graph Examples

The following are the examples of streaming processing.

1. STAllGatherExample
2. STAllReduceExample
3. STBroadCastExample
4. STGatherExample
5. STKeyedGatherExample
6. STKeyedReduceExample
7. STPartitionExample
8. STPartitionKeyedExample
9. STReduceExample
10. StreamingTaskExampleKafka
 
Each example explains the streaming operation with the respective dataflow communication operations 
namely gather, reduce, broadcast, keyedgather, keyedreduce, partition, keyedpartition, and allreduce.

## Batch Task Graph

Batch processing refers the process of handling bounded sequence of data units. Batch applications 
mainly consumes bounded data units and produces the data units. The batch task graph is mainly 
responsible for building and executing the batch applications.

### Batch Task Graph Creation

This is the code snippet for the batch task graph creation with all-reduce dataflow communication
operations.

```text
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);

    String edge = "edge";
    BaseSource g = new SourceBatchTask(edge);
    BaseSink r = new AllReduceSinkTask();

    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.allreduce(SOURCE, edge, Op.SUM, DataType.INTEGER);
    return taskGraphBuilder;
  }
```

### Batch Task Graph Examples

The following are the examples of batch processing.

1. BTAllGatherExample
2. BTAllReduceExample
3. BTBroadCastExample
4. BTGatherExample
5. BTKeyedGatherExample
6. BTKeyedReduceExample
7. BTPartitionExample
8. BTPartitionKeyedExample
9. BTReduceExample

Each example explains the batch operation with the respective dataflow communication operations 
namely gather, reduce, broadcast, keyedgather, keyedreduce, partition, keyedpartition, and allreduce.

## Iterative Task Graph

The iterative task graph computation is mainly useful to perform the iterative computation process 
in the
big data world. It generally captures the complex relationship between the entities.  

### Iterative Task Graph Example

```text
       IterativeSourceTask g = new IterativeSourceTask();
       PartitionTask r = new PartitionTask();
```

```text
     TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
     graphBuilder.addSource("source", g, 4);
     ComputeConnection computeConnection = graphBuilder.addSink("sink", r, 4);
     computeConnection.partition("source", "partition", DataType.OBJECT);
     graphBuilder.setMode(OperationMode.BATCH);
 
     DataFlowTaskGraph graph = graphBuilder.build();
     for (int i = 0; i < 10; i++) {
       ExecutionPlan plan = taskExecutor.plan(graph);
       taskExecutor.addInput(graph, plan, "source", "input", new DataObjectImpl<>(config));
 
       // this is a blocking call
       taskExecutor.execute(graph, plan);
       DataObject<Object> dataSet = taskExecutor.getOutput(graph, plan, "sink");
       DataPartition<Object>[] values = dataSet.getPartitions();
       LOG.log(Level.INFO, "Values: " + values);
     }
```

Run this example with the following command

./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.batch.IterativeJob 

## MultipleComputeNodes Task Graph

The multicompute nodes task graph which consists of a source and sends output to multiple compute 
dataflow nodes. Also, the sink task receives the input from multiple compute dataflow nodes.

The structure of the graph is given below:

  ![MultipleComputeNodes Task Graph](MultiCompute_TaskGraph.png)
 
### MultiCompute Task Graph Example

This example is described in four stages namely 
1. defining the task graph
2. creating the compute connections 
3. creating the communication edges between the compute connections 
4. build and execute the task graph

```text
    SourceTask sourceTask = new SourceTask();
    FirstComputeTask firstComputeTask = new FirstComputeTask();
    SecondComputeTask secondComputeTask = new SecondComputeTask();
    ReduceTask reduceTask = new ReduceTask();
```

```text
    builder.addSource("source", sourceTask, parallel);
    ComputeConnection firstComputeConnection = builder.addCompute(
        "firstcompute", firstComputeTask, parallel);
    ComputeConnection secondComputeConnection = builder.addCompute(
        "secondcompute", secondComputeTask, parallel);
    ComputeConnection reduceConnection = builder.addSink("sink", reduceTask, parallel);
```

The source task creates the direct communication edge beween the first compute and second compute 
task. From the first compute and second compute, it creates an all-reduce communication edge to 
the reduce task.

```text
    firstComputeConnection.direct("source", "fdirect", DataType.OBJECT);
    secondComputeConnection.direct("source", "sdirect", DataType.OBJECT);
    reduceConnection.allreduce("firstcompute", "freduce", new Aggregator(), DataType.OBJECT);
    reduceConnection.allreduce("secondcompute", "sreduce", new Aggregator(), DataType.OBJECT);
```

This build and generate the task graph for the batch process. Then, it call the taskscheduler and 
taskexecutor to build the task schedule plan and execution plan respectively. Finally, it calls
the execute method to execute the generated task graph. 
 
```text
    builder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph graph = builder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);
    taskExecutor.execute(graph, plan);
```

Run this example with the following command

./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.internal.taskgraph.MultiComputeTaskGraphExample -dsize 100 -parallelism 2 -workers 2 
