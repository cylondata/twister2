---
id: 0.2.2
title: Twister2 Release 0.2.2
sidebar_label: Twister2 Release 0.2.2
---


This is a patch release of Twister2 with first versions of few major features.

You can download source code from [Github](https://github.com/DSC-SPIDAL/twister2/releases)

## First version of major features

1. Streaming windowing support
2. Join operations included in Task API
3. Run OpenMPI programs inside task graph
4. Checkpointing for streaming and batch applications

## Minor features

Apart from these, we have done API refactorings and many improvements to performance

## Next Release

We are working on to consolidate the features introduced in this release. Also we are continuing to
improve the code, fix bugs etc.

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
9. Connected DataFlow (Experimental)
    1. Supports creation of multiple dataflow graphs executing in a single job