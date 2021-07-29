---
id: 0.8.0
title: Twister2 Release 0.8.0
sidebar_label: Twister2 Release 0.8.0
---


This is a major release of Twister2. 

You can download source code from [Github](https://github.com/DSC-SPIDAL/twister2/releases)

## Features of this release

1. Fault Tolerance enhancements; Automated fault detection and recovery
2. Table API(experimental)
3. TSet API improvements; Pipe capability and TSetEnvironement
 
## Minor features

Apart from these, we have done many code improvements and bug fixes.

## Next Release

In the next release we are working to,

* Improve and release Table API
* TSQL; Adding SQL support

## Components in Twister2

We support the following components in Twister2

1. Resource provisioning component to bring up and manage parallel workers in cluster environments
    1. Standalone
    2. Kubernetes
    3. Mesos
    4. Slurm 
    5. Nomad
2. Parallel and Distributed Operators in HPC and Cloud Environments
    1. Twister2:Net - a data level dataflow operator library for streaming and large scale batch analysis
    2. Harp - a BSP (Bulk Synchronous Processing) innovative collective framework for parallel applications and machine learning at message level
    3. OpenMPI (HPC Environments only) at message level
3. Task System
    1. Task Graph 
       * Create dataflow graphs for streaming and batch analysis including iterative computations 
    2. Task Scheduler - Schedule the task graph into cluster resources supporting different scheduling algorithms
       * Datalocality Scheduling
       * Roundrobin scheduling
       * First fit scheduling
    3. Executor - Execution of task graph     
       * Batch executor
       * Streaming executor
4. TSet for distributed data representation (Similar to Spark RDD, Flink DataSet and Heron Streamlet)
    1. Iterative computations
    2. Data caching
5. APIs for streaming and batch applications
    1. Operator API
    2. Task Graph based API
    3. TSet API
6. Support for storage systems
    1. HDFS
    2. Local file systems
    3. NFS for persistent storage
7. Web UI for monitoring Twister2 Jobs
8. Apache Storm Compatibility API
9. Apache BEAM API
10. Connected DataFlow (Experimental)
    1. Supports creation of multiple dataflow graphs executing in a single job