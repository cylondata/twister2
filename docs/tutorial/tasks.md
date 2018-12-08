<img align="left" width="125" height="125" src="fox.png">


### Geoffrey C. FOX

# Task Scheduling and Execution (Fault Tolerance)


## Task System

The task layer provides a higher-level abstraction on top of the communication layer to hide the details of execution
and communication from the user, while still delegating data management to the user. At this layer, computations are modeled as task graphs which
can be created either statically as a complete graph or dynamically as the application progresses. The task system comprises
of task graph, execution graph, and task scheduling process.

#### Task Graph

A node in the task graph represents a task while an edge represents a communication link between nodes. Each node in the graph holds information
about the inputs and its outputs (edges). Also, a node contains an executable user code. The user code in a task is executed when events arrive at the
inputs of the task. The user will output events to the output edges of the task graph and they will be sent through the network by the communication
layer. A task can be long-running or short-running depending on the type of application. For example, a stream graph will have long running tasks
while a dataflow graph without loops will have short running tasks. When loops are present, long-running tasks can be appropriate to reduce task
creation overheads.

#### Execution Graph

Execution graph is a transformation of the user-defined task graph, created by the framework for deploying on the cluster. This execution graph will
be scheduled onto the available resource by the task scheduler. For example, some user functions may run on a larger number of nodes depending on
the parallelism specified. Also, when creating the execution graph, the framework can perform optimizations on the user graph to increase efficiency
by reducing data movement and overlapping I/O and computations.

#### Task Scheduling

Task scheduling is the process of scheduling multiple task instances into the cluster resources. The task scheduling in Twister2 generates the task
schedule plan based on the per job policies, which places the task instances into the processes spawned by the resource scheduler. It aims to allocate
a number of dependent and independent tasks. Moreover, task scheduling requires different scheduling methods for the allocation of tasks
and resources based on the architectural characteristics. The selection of the best method is a major challenge in the big data processing environment.
The task scheduling algorithms are broadly classified into two types, namely static task scheduling algorithms and dynamic task scheduling
algorithms. Twister2 aims to support both types of task scheduling algorithms.

<span style="color: green"> More content will be added soon.... </span>

