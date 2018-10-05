# Twister2 Release 0.1.0

Twister2 0.1.0 is the first public release of Twister2. We are excited to bring a high performance data analytics
hosting environment that can work in both cloud and HPC environments. This is the first step towards 
building a complete end to end high performance solution for data analytics ranging from streaming to batch analysis to 
machine learning applications. 

## Major Features

This release includes the core components of realizing the above goals. 

1. Resource provisioning component to bring up and manage parallel workers in cluster environments
    1. Standalone
    2. Kubernetes
    3. Mesos
    4. Slurm
    5. Nomad
2. Parallel and Distributed Communications
  1. Twister2:Net - a dataflow communication library for streaming and large scale batch analysis
  2. Harp - a BSP collective framework for parallel applications and machine learning
3. Task Graph - Create dataflow graphs for streaming and batch analysis 
4. Task Scheduler - Schedule the task graph in to cluster resources 
5. Executor - Execution of task graph       
6. API for creating Task Graph and Communication

These features translates to running following types of applications natively with high performance.

1. Streaming computations
2. Data operations in batch mode
3. Iterative computations

We achieve all these feature while supporting both cloud and HPC environments. When deployed in an
HPC environment, Twister2 can use features such as advanced hardware to optimize its performance while
for cloud deployments we use high performance techniques to optimize our performance.

## Next Release

We have started working on our next major release that will connect the core components we have developed 
into a full data analytics environment. In particular it will focus on providing APIs around the core
capabilities of Twister2 and integration of applications in a single dataflow. 

1. Hierarchical task scheduling - Ability to run different types of jobs in a single dataflow
2. Fault tolerance
3. Data API including DataSet similar to Spark RDD, Flink DataSet and Heron Streamlet
3. Supporting different API's including Storm, Spark, Beam  
4. Heterogeneous resources allocations
5. Web UI for monitoring Twister2 Jobs
6. More resource managers - Pilot Jobs, Yarn
7. More example applications



    


 