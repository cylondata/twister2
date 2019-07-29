---
id: op_api
title: Operator API
sidebar_label: Operator API
---

This is the lowest level of API provided by Twister2. It provides the basic parallel operators required
by a parallel programs in terms of both Bulk Synchronous Parallel (BSP) and DataFlow API. 

The BSP APIs are provided by Harp and MPI specification (OpenMPI).

The DataFlow operators are implemented by Twister2 as a Twister:Net library.

We will focus on the DataFlow operators in this guide as Harp API's and MPI APIs are discussed in their documentation.

## DataFlow Operator Overview

Twister2 dataflow operators are designed as asynchronous operators with state.
Every operator needs a ```LogicalPlan```, a ```Communicator``` and set of ```sources``` and set of ```targets```.

```LogicalPlan``` gives the mapping from set of ids to workers processes. These logical ids are
used by sources and targets. An operator can have sources in one set of processes
and targets in another set of processes. Depending on the operator, there source set of target
set can have one or more logical ids.

The ```communicator``` encapsulates the underlying channel that is being used for network data transfer.
At the moment Twister:Net supports MPI based and TCP socket based implementations.

## An Example Operator

Lets look at an example operator. For this example we have chosen the batch Reduce operator.
This operator can have many sources and only one target.

Input to the operator can be any data type. All the sources input data and these values are reduced
to a single value at the target. How to reduce multiple values to a single value is defined by
a user defined function.

```java
    // setup the batch operation
    BReduce reduce = new BReduce(communicator, logicalPlan, sources, target,
        reduceFunction, reduceReceiver, datatype);
    // send the data
    reduce.reduce(task, data, flag);

    // lets call finish, every source need to call finish, in this example
    // we assume one source in a single worker
    reduce.finish(source);

    // wait while operator completes
    while (!reduce.isComplete()) {
        reduce.progressChannel();
    }
```

After every participating source sends its data and calls finish, the final values will be
received to the user defined reduceReceiver. At this point, the operation will be complete
and the while loop at the end will exit.

We can call ```reduce.reduce``` multiple times.

## Streaming & Batch Operators

There are separate set of streaming operators and batch operators. The streaming operators never end
while batch operators have an end. Otherwise their programming is similar. So in our example above
we won't have the ```finish``` call and ```isComplete``` will never be ```true``` for a streaming
operator.

For batch operators user needs to indicate the end by using a special flag or a method.

## Termination & Progress

The operator library doesn't created its own threads. So, unless user calls some function in the library,
nothing will automatically happen. The progression of the operator is done using the
```progress``` and ```progressChannel``` methods. Unless these methods are called, data will
not go through the network.

The data accepting functions of the operators can return ```true``` or ```false``` depending
on weather they accept the data or not. If the method doesn't accept data, that means the internal
data structures are full and user needs to call the ```progressChannel``` method to send the data to
its targets. User doesn't need to wait unti the method returns ```false``` to call the progression
methods.

## High performance communication layer

Because of the bottom up approach taken when designing and implementing Twister2 the communication 
layer performs extremely well. A complete study on the Twister2 communication layer can be found at
[Twister2:Net](https://www.computer.org/csdl/proceedings/cloud/2018/7235/00/723501a383-abs.html). 

The image below which is extracted from [Twister2:Net](https://www.computer.org/csdl/proceedings/cloud/2018/7235/00/723501a383-abs.html) shows how
Twister2 performs against Apache Spark and MPI. Please note that Spark KMeans example is written using the data level API 
while Twister2 and MPI implementations are communication level applications. However it is clear that Twister2 performs on the same
level as OpenMPI which is an highly optimized communication library in the HPC world. And it out performs Spark by roughly a factor of x10.

![Kmeans Performance Comparison](assets/kmeans_comparison_low.png)

Notation :   
`DFW` refers to Twister2  
`BSP` refers to MPI (OpenMPI)  
 