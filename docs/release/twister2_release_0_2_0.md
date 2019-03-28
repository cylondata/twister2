# Twister2 Release 0.2.0

Twister2 0.2.0 is the second open source public release of Twister2. We are excited to bring another release of our 
high performance data analytics hosting environment that can work in both cloud and HPC environments. 

You can download source code from [Github](https://github.com/DSC-SPIDAL/twister2/releases)

## Major Features

This release includes the core components of realizing the above goals. 

1. Resource provisioning component to bring up and manage parallel workers in cluster environments
    1. Standalone
    2. Kubernetes
    3. Mesos
    4. Slurm 
    5. Nomad
2. Parallel and Distributed Communications in HPC and Cloud Environments
    1. Twister2:Net - a data level dataflow communication library for streaming and large scale batch analysis
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
4. API for creating Task Graph and Communication
    1. Communication API
    2. Task based API
    3. Data API (TSet API)
5. Support for storage systems
    1. HDFS
    2. Local file systems
    3. NFS for persistent storage
6. Web UI for monitoring Twister2 Jobs
7. Apache Storm Compatibility API
    
These features translates to running following types of applications natively with high performance.

1. Streaming computations
2. Data operations in batch mode
3. Iterative computations

## Examples

With this release we include several examples to demonstrate various features of Twister2.

1. A Hello World example
2. Communication examples - how to use communications for streaming and batch
3. Task examples - how to create task graphs with different operators for streaming and batch
4. K-Means 
5. Sorting of records
6. Word count 
7. Iterative examples
8. Harp example
9. SVM

## Road map

We have started working on our next major release that will connect the core components we have developed 
into a full data analytics environment. In particular it will focus on providing APIs around the core
capabilities of Twister2 and integration of applications in a single dataflow. 

### Next Major Release (End of June 2019)

1. Connected DataFlow
2. Fault tolerance
3. Supporting more API's including Beam  
4. Python API
6. More resource managers - Pilot Jobs, Yarn
7. More example applications

### Beyond next release

1. Implementing core parts of Twister2 with C/C++ for high performance 
3. Direct use of RDMA
5. SQL interface 
6. Native MPI support for cloud deployments

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
