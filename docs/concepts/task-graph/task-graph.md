# Task Graph

## Taskgraph in Twister2

The task layer provides a higher-level abstraction on top of the communication layer to hide the 
underlying details of the execution and communication from the user. Computations are modeled as 
task graphs in the task layer which could be created either statically or dynamically. A node in the 
task graph represents a task whereas an edge represents the communication link between the vertices.
Each node in the task graph holds the information about the input and its output. A task could be 
long-running (streaming graph) or short-running (dataflow graph without loops) depending on the type 
of application. 

A task graph 'TG' generally consists of set of Task Vertices'TV' and Task Edges \(TE\) which is 
mathematically denoted as Task Graph

```text
(TG) -> (TV, TE)
```
## Static and Dynamic Task Graphs 
The task graphs can be defined in two ways namely static and dynamic task graph. 
 * Static task graph - the structure of the complete task graph is known at compile time.
 * Dynamic task graph - the structure of the task graph does not know at compile time and the program dynamically define the structure of the task graph during run time.

 * Static Task Graph in Big Data Systems
   * Apache Flink
   * Apache Heron
   * Apache Storm
 * Dynamic Task Graph in Big Data Systems
   * Apache Spark
  
The following three essential points should be considered while creating and scheduling the task instances of the task graph.

1. Task Decomposition - Identify independent tasks which can execute concurrently
2. Group tasks - Group the tasks based on the dependency of other tasks.
3. Order tasks - Order the tasks which will satisfy the constraints of other tasks.

\(Reference: Patterns for Parallel Programming, Chapter 3 \(2\) & [https://patterns.eecs.berkeley.edu/?page\_id=609](https://patterns.eecs.berkeley.edu/?page_id=609)\)

## Task Graph in Twister2

* The task graph system in Twister2 is mainly aimed to support the directed dataflow task graph which consists of task vertices and task edges. 
   * The task vertices represent the source and target task vertex 
   * The task edge represent the edges to connect the task vertices
* The task graph in Twister2  
  * supports iterative data processing - For example, in K-Means clustering algorithm, at the end of every iteration, data points and centroids are stored in the DataSet which will be used for the next iteration 
  * It doesn’t allow loops or self-loops or cycles
  
## Directed Task Graph and Undirected Task Graph

There are two types of task graphs namely directed task graph and undirected task graph. In directed task graph, 
the edges in the task graph that connects the task vertexes have a direction as shown in Fig.1 whereas in 
undirected task graph, the edges in the task graph that connects the task vertexes have no direction as shown in Fig 2.
The present task system supports only directed dataflow task graph.

![Directed Graph](directed.png)  ![UnDirected Graph](undirected.png)

## Dataflow Task Graph

It consists of set of task vertices and task edges. It describes the details about how the data is 
consumed (in this example using all-reduce communication) between the task vertices.  

 * Source Task - It extends the BaseSource and implements the Receptor interface which is given below.
 * Compute Task - It implements the IFunction interface which is given below.
 * Sink Task - It extends the BaseSink and implements the Collector interface. 

## Implementation Details

### ITaskGraph

It is the main interface which is primarily responsible for creating task vertexes and task edges between those vertexes, removing task vertexes and task edges, and others.

### BaseDataflowTaskGraph

It is the base class for the dataflow task graph which consists of methods to find out the inward and outward task edges and incoming and outgoing task edges. 
Validates the task vertexes and creates the directed dataflow edge between the source and target task vertexes.
Performs the validation such as duplicate names for the task, duplicate edges between same two tasks, self-loop in the task graph, and cycles in the task graph. 
Some of the main methods available in this class are

```text
            addTaskVertex(TV sourceTaskVertex, TV targetTaskVertex)

            addTaskEge(TV sourceTaskVertex, TV targetTaskVertex, TE taskEges)

            removeTaskVertex(TV taskVertex), removeTaskEdge(TE taskEdge)

            validateTaskVertex(TV source/target vertex)
            
            boolean detectSelfLoop(Set<TV> taskVertex)
            
            detectCycle(TV vertex, Set<TV> taskVertexSet, Set<TV> sourceTaskSet, Set<TV> targetTaskSet)
            
            boolean containsTaskEdge(TE taskEdge)
```

### DataflowTaskGraph

It is the main class which extends the BaseDataflowTaskGraph, first it validate the task graph then store the directed edges into the task map which consists of source task vertex and target task vertex..

### Vertex

It represents the characteristics of a task instance. It consists of task name, cpu, ram, memory, parallelism, and others.

### Edge

Edge represents the communication operation to be performed between two task vertices. It consists of edge name, type of operation, operation name, and others.

### GraphBuilder

The graph builder is the mainly responsible for creating the dataflow task graph which has the methods for connecting the task vertexes, add the configuration values, setting the parallelism, and validate the task graph.

### Operation Mode

The operation mode supports two types of task graphs namely streaming and batch.

