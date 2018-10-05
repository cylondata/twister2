# Twister2 Release 0.1.0

Twister2 0.1.0 is the first public release of Twister2. We are excited to bring a high performance data analytics
hosting environment that can work in both cloud and HPC environments. This is the first step towards 
building a complete end to end high performance solution for data analytics ranging from streaming to batch analysis to 
machine learning applications. Our vision is to make the system work seamlessly both in cloud and HPC environments ranging from single machines to large clusters.

## Major Features

This release includes the core components of realizing the above goals. 

1. Resource provisioning component to bring up and manage parallel workers in cluster environments
    1. Standalone
    2. Kubernetes
    3. Mesos
    4. Slurm 
    5. Nomad
2. Parallel and Distributed Communications in HPC and Cloud Environments
    1. Twister2:Net - a dataflow communication library for streaming and large scale batch analysis
    2. Harp - a BSP (Bulk Synchronous Processing) collective framework for parallel applications and machine learning
    3. OpenMPI (HPC Environments only)
3. Task Graph - Create dataflow graphs for streaming and batch analysis including iterative computations
4. Task Scheduler - Schedule the task graph into cluster resources supporting different scheduling algorithms
    1. Datalocality Scheduling
    2. Roundrobin scheduling
    3. First fit scheduling
5. Executor - Execution of task graph     
    1. Batch executor
    2. Streaming executor
6. API for creating Task Graph and Communication
    1. Communication API
    2. Task based API
7. Support for storage systems
    1. HDFS
    2. Local file systems
    3. NFS for persistent storage
    
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

## Road map

We have started working on our next major release that will connect the core components we have developed 
into a full data analytics environment. In particular it will focus on providing APIs around the core
capabilities of Twister2 and integration of applications in a single dataflow. 

### Next release (End of December 2018)

1. Hierarchical task scheduling - Ability to run different types of jobs in a single dataflow
2. Fault tolerance
3. Data API including DataSet similar to Spark RDD, Flink DataSet and Heron Streamlet
3. Supporting different API's including Storm, Spark, Beam  
4. Heterogeneous resources allocations
5. Web UI for monitoring Twister2 Jobs
6. More resource managers - Pilot Jobs, Yarn
7. More example applications

### Beyond next release

1. Implementing core parts of Twister2 with C/C++ for high performance 
2. Python APIs
3. Direct use of RDMA
4. FaaS APIs 
5. SQL interface 



    


 
