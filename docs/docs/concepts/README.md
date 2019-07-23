---
id: concepts
title: Concepts Overview
sidebar_label: Concepts
---

Twister2 consists of several core components that provide the essential features of a data analytics platform.

### Resource provisioning component 

The primary responsibility of this component is job submission and management. It provides abstractions 
to acquire resources and manage the life cycle of a parallel job. We have developed links to the 
following resource managers and would like to add others such as Yarn, Marathon in the future.

  1. Standalone
  2. Kubernetes
  3. Mesos
  4. Slurm
  5. Nomad
  
Twister2 jobs run in isolation without sharing any resources among jobs. Unlike in other big data 
systems such as Spark or Flink, Twister2 can be running in HPC or cloud environments.

### Parallel and Distributed communication operators 

Unlike other big data analytics systems, we recognize the importance of network communication 
operators for parallel applications and provide a set of abstractions and implementation that 
satisfy the needs of different applications. Both streaming operators and batch operators are provided. 
Three types of operator implementations are provided to the user.  

  1. Twister:Net - a data level dataflow operator library for streaming and large scale batch analysis
  2. Harp - a BSP (Bulk Synchronous Processing) innovative collective framework for parallel applications and machine learning at the message level
  3. OpenMPI (HPC Environments only) at the message level
  
In other big data systems, communication operators are hidden behind high-level APIs. This design 
gives a priority to operator API’s and allows the development of different methods to optimize the 
performance while users can use low-level APIs to program high-performance applications. Twister:Net 
can use both socket based or MPI based (ISent/IRecv) to communicate allowing it to perform well on advanced hardware.

### Task System

This component provides the abstractions to hide the execution details and an easy to program API for parallel applications. A user can create a streaming task graph or a batch task graph to analyze data. The abstractions have similarities to Storm and Hadoop APIs. Task system consists of the following major components.

Task Graph - Create dataflow graphs for streaming and batch analysis including iterative computations
Task Scheduler - Schedule the task graph into cluster resources supporting different scheduling algorithms
Executor - Batch and streaming executions

In Spark and Flink, this component is hidden from the user. Apache Storm and Flink API’s are at this level of abstraction. We allow pluggable executors and task schedulers to extend the capabilities of the system.
  
### Distributed Data Abstraction

A typed distributed data abstraction, similar to Spark RDD, BEAM PCollections or Heron Streamlet is provided here. It can be used to program a data pipeline, a streaming application or iterative application.

  1. Iterative computations
  2. Streaming computations
  3. Data pipelines

In Twister2, iterations (for loops) are carried on each worker. Spark uses a central driver to control the iterations, which can lead to poor performance for applications with frequent iterations (less computation in an iteration). Flink uses the task graph itself to code the iterations (cyclic graphs) and doesn’t support nested iterations.

Data pipelines in Twister2 are similar to Flink or Spark. Twister2 is a pure streaming engine making it similar to Flink or Storm (not a minibatch system like Spark).

### Auxiliary Components

Apart from the main futures, it provides the following components.

  1. Web UI for monitoring Jobs
  2. Connected DataFlow (Experimental), this is for workflow type jobs
  3. Data access API for connecting to different data sources (File systems)

## APIs

Twister2 supports several APIs for programming an application. An application can use a mixed set of these APIs. The main APIs include

1. TSet API (Similar to Spark).
2. Task Graph-based API (Similar to Hadoop and Storm)
3. Communications Operator API (BSP API and DataFlow operator API)
 
Twister2 supports the Storm API for programming a Storm application. The Twister2 native streaming API provides a richer set of operations than Storm API.

These concepts are widely discussed in the [publications section](../publications.md)