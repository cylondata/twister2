---
id: concepts
title: Concepts Overview
sidebar_label: Overview
---

This document describes the parallel and distributed concepts applied to Twister2. As a distributed 
application development framework, it provides API's for creating distributed applications 
including streaming and batch. Next as a distributed execution engine, it can map these applications
to set of compute resources and execute them.  

## Compute Resources

In general a Twister2 Application executes on set of nodes in a cluster. 
These nodes may consist of physical machines, virtual machines or containers. 

From Twister2 point of view each of these nodes have the following components.

* One or more CPUs (Usually we call one CPU a socket)
* Each CPU can have one or more cores (a core is a processing unit)
* Random access memory (possibly in Numa configuration)
* One or more storage units (hard disks)
* Nodes are connected using a TCP network
* Optionally has a high performance network such as Infiniband or Omni-Path

Twister2 has options for running an application optimally on such an environment. In general we call
the node that submits the job, a client node (driver node) and the nodes that executes the parallel 
computation, cluster nodes.  

## Twister2 Job

A user writes the source code of a Twister2 Job using the its APIs. Every Twister2 job starts 
with IWorker instance.

A Twister2 job consists of three components. 

1. Job client
2. Job master
3. Set of IWorker instances

Job client and job master can run in a single process (in client node) or can be running in 
different processes. In the latter case, job master runs inside the cluster nodes. Each job has its
own job master. 

Each IWorker instance is a separate process. An IWorker instance may employ a single thread to execute
the user code or multiple threads. We can allocate resources to IWorker instances such as memory,
number of CPUs depending on the requirements of the computation. For example if an IWorker uses 2 threads
to execute the user code, we can allocate atmost 2 CPU cores to an IWorker. Beyond 2 cores, it won't 
be able to use as it only has 2 threads.


