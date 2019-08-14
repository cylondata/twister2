---
id: task_api
title: Compute API
sidebar_label: Compute API
---

The Compute API is the middle tier API that provides both flexibility and performance. A user directly
models an application as a graph and program it using the graph constructs.   

### Overview of Compute API

The ```TaskGraphBuilder``` is the entry point for the task compute API which helps the user to define their 
application in terms of a graph. Every computation with this API, consists of at least a ```Source``` task. 
One can have ```Compute``` tasks and ```Sink``` tasks in the same computation.  

The API defines methods to create the tasks as well as links between them. A link between two tasks ultimately
translates to a distributed operations such as a ```reduce``` or a ```gather```. The types of links supported 
by the system is predefined and one can extend the system by adding additional links as well.

Once the compute graph is defined, it needs be scheduled and executed. The framework provides a set of predifined schedulers
and executors. One can add their own schedulers and executors as well.

### Streaming & Batch

The computation graph as an option to set weather it is going to do a streaming or batch computation. 
Once this is set the executors and schedulers can act accordingly.  

### Example Program

The following shows an example program based on the compute API.



#### Creating and Initializing the TaskGraph Builder

```java 
TaskGraphBuilder computeGraphBuilder = TaskGraphBuilder.newBuilder(config);
```

#### API for defining Source Task 

The method to define the source task with the task parallelism is 

```java  
public SourceConnection addSource(String name, ISource source, int parallel)
public SourceConnection addSource(String name, ISource source)
```
#### Defining Source Task 

```java 
SourceTask sourceTask = new SourceTask();
taskgrapbuilder.addSource("sourcetaskname", sourceTask, parallelismvalue);
```
 
#### API for defining Compute Task

The method to define the compute task with the task parallelism is

```java 
public ComputeConnection addCompute(String name, ICompute compute, int parallel)
public ComputeConnection addCompute(String name, ICompute compute)
``` 

#### Defining Compute Task 
```java 
ComputeTask computeTask = new ComputeTask();
ComputeConnection computeConnection = taskgrapbuilder.addCompute("computetaskname", computeTask, parallelismValue);
```

#### API for defining Sink Task

The method to define the sink task with the task parallelism is 

```java 
public ComputeConnection addSink(String name, ISink sink, int parallel)
public ComputeConnection addSink(String name, ISink sink) 
public ComputeConnection addSink(String name, IWindowedSink sink, int parallel)
```

#### Defining Sink Task

```java 
SinkTask sinkTask = new SinkTask();
ComputeConnection computeConnectionSink = computeGraphBuilder.addSink("sinktaskname", sinkTask, parallelismValue)
```

#### Defining the Task Edges

It is essential to create the task edges between the source, compute, and sink tasks. If the user
task graph has only source and sink tasks they may have to establish the communication between
the source and sink tasks. 

#### API for defining Task Edges

This API is for creating the "direct" communication edge between the source, compute, and sink tasks. It 
could be used for creating the "direct" communication edge between the source and sink tasks. The 
ComputeConnection has various methods to establish the communication edges namely "broadcast", "reduce",
"allreduce", "allgather", "partition", "keyedreduce, "keyedgather", and "keyedpartition". 

```java 
 public ComputeConnection direct(String parent, String name, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.DIRECT, dataType);
    inputs.put(parent, edge);
    return this;
  }
```

For defining other communication perations, please look into 

```java 
edu.iu.dsc.tws.api.task.ComputeConnection
```

#### Task Edge Creation Example

```java 
computeConnection.direct("sourcetaskname", Context.TWISTER2_DIRECT_EDGE, DataType.OBJECT);
computeConnectionSink.direct("computetaskname", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
```

#### Setting the operation Mode

The user can define the task graph as either "batch" or "streaming" mode using the OperationMode.

```java 
builder.setMode(OperationMode.BATCH);
```
 
#### Building the taskgraph

Once the task graph is defined, the user has to call the build() to build the defined 
 task graph. 
 
 ```java 
   DataFlowTaskGraph taskGraph = computeGraphBuilder.build();
 ```
 
#### Task Graph Execution

The user pass the generated task graph to the TaskExecutor to get the execution plan. The task executor
first calls the Task Scheduler API to retrieve the task schedule plan. The task schedule plan consists of
the container/worker details and allocation of task instances to each container/worker. Next, using the 
task schedule plan the ExecutionPlanBuilder generate the task execution plan. 
 
 ```java 
ExecutionPlan executionPlan = taskExecutor.plan(taskGraph);
taskExecutor.execute(taskGraph, executionPlan);
```
 
#### Task API Example 

This is the task API example for reading the data points from the file, partition the data points, 
and return the data points in an array which consists of source, compute, and sink tasks.

```java 
    //Defining source, compute, and sink tasks.
    DataObjectSource dataObjectSource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    DataObjectCompute dataObjectCompute = new DataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue, dimension);
    DataObjectDirectSink dataObjectSink = new DataObjectDirectSink();
    TaskGraphBuilder computeGraphBuilder = TaskGraphBuilder.newBuilder(config);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsComputeGraphBuilder.addSource("datapointsource", dataObjectSource, parallelismValue);
    ComputeConnection computeConnection = computeGraphBuilder.addCompute(
        "datapointcompute", dataObjectCompute, parallelismValue);
    ComputeConnection computeConnectionSink = computeGraphBuilder.addSink(
        "datapointsink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    computeConnection.direct("datapointsource", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    computeConnectionSink.direct("datapointcompute", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    datapointsComputeGraphBuilder.setMode(OperationMode.BATCH);
    
    //Build the first taskgraph
    DataFlowTaskGraph datapointsTaskGraph = datapointsComputeGraphBuilder.build();
    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
            datapointsTaskGraph, firstGraphExecutionPlan, "datapointsink");

``` 
 
 
### Task API Example for wordcount 

This is the task API example for calculating the wordcount which has only source and sink tasks.

```java 
  @Override
  public void execute() {
    // source task
    WordSource source = new WordSource();
    // sink task
    WordAggregator counter = new WordAggregator();

    // build the task graph
    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(config);
    // add the source with parallelism of 4
    builder.addSource("word-source", source, 4);
    // add the sink with parallelism of 4 and connect it to source with keyed reduce operator
    builder.addSink("word-aggregator", counter, 4).keyedReduce("word-source", EDGE,
        new ReduceFn(Op.SUM, DataType.INTEGER), DataType.OBJECT, DataType.INTEGER);
    // set the operation mode to batch
    builder.setMode(OperationMode.BATCH);

    // execute the graph
    DataFlowTaskGraph graph = builder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);
    taskExecutor.execute(graph, plan);
  }

```
