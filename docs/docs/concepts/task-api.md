---
id: task_api
title: Task API
sidebar_label: Task API
---

The Task API is the middle tier API that provides both flexibility and performance. A user directly
models an application as a graph and program it using the Task Graph API.  

### Overview of Task Graph API

The TaskGraphBuilder is the entry point for the task graph API which helps the user to define their 
application in terms of task graph. It consists of various methods to define the source task, compute 
task, and sink task. Also, it provides the methods to define the edges between the source, compute, 
and sink tasks. The TaskGraphBuilder creates the corresponding task vertices for those tasks. In 
addition to that, the user can define their task constraints and the type of the tasks such as 
Streaming or Batch using the Task Graph API. 

### Defining

#### Creating and Initializing the TaskGraph Builder

```java 
TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
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
ComputeConnection computeConnectionSink = taskGraphBuilder.addSink("sinktaskname", sinkTask, parallelismValue)
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
   DataFlowTaskGraph taskGraph = taskGraphBuilder.build();
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
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsTaskGraphBuilder.addSource("datapointsource", dataObjectSource, parallelismValue);
    ComputeConnection computeConnection = taskGraphBuilder.addCompute(
        "datapointcompute", dataObjectCompute, parallelismValue);
    ComputeConnection computeConnectionSink = taskGraphBuilder.addSink(
        "datapointsink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    computeConnection.direct("datapointsource", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    computeConnectionSink.direct("datapointcompute", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    datapointsTaskGraphBuilder.setMode(OperationMode.BATCH);
    
    //Build the first taskgraph
    DataFlowTaskGraph datapointsTaskGraph = datapointsTaskGraphBuilder.build();
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
