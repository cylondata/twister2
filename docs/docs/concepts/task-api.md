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

Once the compute graph is defined, it needs be scheduled and executed. The framework provides a set of predefined 
schedulersand executors. One can add their own schedulers and executors as well.

### Streaming & Batch

The computation graph as an option to set whether it is going to do a streaming or batch computation. 
Once this is set the executors and schedulers can act accordingly.  

### Example Program

The following pseudocode demonstrates the use of compute API. First user creates the ```ComputeEnvironment```.
Then a ```ComputeGraphBuilder``` is created and the graph is constructed. After this tasks are added to the 
graph and graph is built. THen we can use the ```ComputeEnvironment``` to execute the graph.

```java
public class HelloTwister2 implements Twister2Worker {
  @Override
  public void execute(WorkerEnvironment workerEnv) {
    ComputeEnvironment cEnv = ComputeEnvironment.init(workerEnv);
    ComputeGraphBuilder computeGraphBuilder = cEnv.newTaskGraph(OperationMode.BATCH);
    
    // build the graph by creating the tasks and adding them
    BaseSource g = new SourceTask();
    ISink r = new ReduceSinkTask();

    // add the sources, ops and connect them
    computeGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = computeGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.reduce(SOURCE)
        .viaEdge("reduce-edge")
        .withOperation(Op.SUM, MessageTypes.INTEGER_ARRAY);
    
    ComputeGraph computeGraph = computeGraphBuilder.build();
    
    // execute the graph
    cEnv.execute(computeGraph);
  }
}
```

### Tasks

A compute graph can have three types of tasks. They are 

* Source task
* Compute task
* Sink task

Every compute graph must have a source task. 

#### TaskContext

Every task is pass a ```TaskContext``` object. This object can be used to write values through connections, get 
information about the task graph and environment. 

#### Source task

A source task marks the beginning of a computation. The execute method of the source tasks are called 
by the executor until the task notifies the framework that it doesn't have anymore input via the ```TaskContext```

#### Compute Task

A compute task does computation based on messages and outputs values via its output edges.

#### Sink Task

Sink task is a leaf node of the graph and cannot have outputs. Other than that it is same as a compute task.

### Compute Connections

Compute connections are edges in the graph that identify different communication channels between the tasks. Each edge 
can have a name (if not specified a default name is assigned). If there are multiple task edges between two tasks,
the user needs to specify their names. 

These are properties supported by each edge. They have a name, a data type and set of properties. Different edges
can add to this base set. 

| Property | How specified | Description | Default      |
| :---     | :---          | :---        | :---         |
| name     | viaEdge       | Name of the edge | default |
| data type | withDataType | Data type of the edge | OBJECT |     
| property | withProperty  | Specify properties    | none |

#### Reduce  

Reduce values in N tasks to a single task.

| Property | How specified | Description | Default      |
| :---     | :---          | :---        | :---         |
| operation     | withOperation       | A predifined operation. These only works on array data types | none |
| funtion     | withReductionFunction       | A user defined function to reduce two values into one | none |

Only one of these properties can be speficied.

#### AllReduce

Reduce values in N tasks to M tasks.

| Property | How specified | Description | Default      |
| :---     | :---          | :---        | :---         |
| operation     | withOperation       | A predifined operation. These only works on array data types | none |
| funtion     | withReductionFunction       | A user defined function to reduce two values into one | none |

Only one of these properties can be speficied.

#### KeyedReduce

Reduce values based on a key. This is a N tasks to M tasks operation. 

| Property | How specified | Description | Default      |
| :---     | :---          | :---        | :---         |
| operation     | withOperation       | A predifined operation. These only works on array data types | none |
| funtion     | withReductionFunction       | A user defined function to reduce two values into one | none |
| key type    | withKeyType      | The data type of the key | OBJECT |
| partitioner | withTaskPartitioner | The partitioner that defines where the data is sent | HashPartitioner |
 
#### Gather 

Gathers values from N tasks to a single task.

No specific properties.

#### AllGather 

Gathers values from N tasks and distributed them to M tasks.

No specific properties.

### Keyed Gather 

This operation gathers values based on keys.

| Property | How specified | Description | Default      |
| :---     | :---          | :---        | :---         |
| operation     | withOperation       | A predifined operation. These only works on array data types | none |
| funtion     | withReductionFunction       | A user defined function to reduce two values into one | none |
| key type    | withKeyType      | The data type of the key | OBJECT |
| partitioner | withTaskPartitioner | The partitioner that defines where the data is sent | HashPartitioner |
| disk    | useDisk | Weather to use disk for operation | false |
| sort    | sortBatchByKey | sort based on keys | false |
| group   | groupBatchByKey | returns values grouped by keyes | true |


#### Direct 

N to N operation where N tasks sends values to N tasks. Each source has a corresponding target tasks. Mostly 
targeted for in-memory operations.

No specific properties

#### Broadcast 

1 to N operation. Broadcast a value from 1 task to N tasks.

No specific properties

#### Partition 

All to all operation that sends values from N tasks to M tasks. 

| Property | How specified | Description | Default      |
| :---     | :---          | :---        | :---         |
| partitioner | withTaskPartitioner | The partitioner that defines where the data is sent | HashPartitioner |
| disk    | useDisk | Weather to use disk for operation | false |
| sort    | sortBatchByKey | sort based on keys | false |
| group   | groupBatchByKey | returns values grouped by keyes | true |

#### Keyed Partition 

All to all operation that sends values from N tasks to M tasks. 

| Property | How specified | Description | Default      |
| :---     | :---          | :---        | :---         |
| operation     | withOperation       | A predifined operation. These only works on array data types | none |
| funtion     | withReductionFunction       | A user defined function to reduce two values into one | none |
| key type    | withKeyType      | The data type of the key | OBJECT |
| partitioner | withTaskPartitioner | The partitioner that defines where the data is sent | HashPartitioner |
| disk    | useDisk | Weather to use disk for operation | false |
| sort    | sortBatchByKey | sort based on keys | false |
| group   | groupBatchByKey | returns values grouped by keyes | true |

### Inputs & Outputs

Every compute graph can have inputs and outputs. These inputs and outputs are marked by two interfaces.

* Receptor
* Collector

A task implementing the ```Receptor``` can accept a named input. A task implementing ```Collector``` interface
should have a named output defined by its contract.