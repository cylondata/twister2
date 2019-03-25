# Programming API's

Twister2 provides three levels of programming at this level. These APIs allow users to program 
a distributed application according to the level of usability and performance needed.

![Twister2 Concepts](../images/api_levels.png) 

This fact is summarized in the following figure.

![Twister2 API Levels](../images/tw2-api-levels.png)

## Operator API

This is the lowest level of API provided by Twister2. It provides the basic parallel operators required
by a parallel programs in terms of both Bulk Synchronous Parallel (BSP) and DataFlow API. 

The BSP APIs are provided by Harp and MPI specification (OpenMPI).

The DataFlow operators are implemented by Twister2 as a Twister:Net library. 

## Task API

The Task API is the middle tier API that provides both flexibility and performance. A user directly
models an application as a graph and program it using the Task Graph API.  

## TSet API

The TSet API is similar to Spark RDD, Flink DataSet or Heron Streamlet APIs. It provides a typed 
functional style API for programming a distributed application. The user program is written as a 
set of data transformation steps. 



