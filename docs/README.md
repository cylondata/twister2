# Twister2

Twister2 provides a data analytics hosting environment where it supports different data analytics 
including streaming, data pipelines and iterative computations. 

Unlike many other big data systems that are designed around user APIs, Twister2 is built from bottom 
up to support different APIs and workloads. Our vision for Twister2 is a complete computing
 environment for data analytics. 
 
One major goal of Twister2 is to provide independent components, that can be used by other 
big data systems and evolve separately. 
 
We support the following components in Twister2

1. Resource provisioning abstraction - Obtain cluster resources
   1. Standalone
   2. Kubernetes
   3. Mesos
   4. Slurm
   5. Nomad
2. Parallel and Distributed Communications
   1. Twister2:Net - a dataflow communication library for streaming and large scale batch analysis
   2. Harp - a BSP collective framework for parallel applications and machine learning
3. Task Graph - Create dataflow graphs 
4. Task Scheduler - Schedule the task graph in to cluster resources
5. Executor - Execution of task graph       
6. API for creating Task Graph and Communication

Twister2 can be deployed both in HPC and cloud environments. When deployed in a HPC environment, it 
can use OpenMPI for its communications. It can be programmed at different levels depending on the 
application types giving the user the flexibility to use underlying features.

## High performance communication layer

Because of the bottom up approach taken when designing and implementing Twister2 the communication 
layer performs extremely well. A complete study on the Twister2 communication layer can be found at
[Twister2:Net](https://www.computer.org/csdl/proceedings/cloud/2018/7235/00/723501a383-abs.html). 

The image below which is extracted from [Twister2:Net](https://www.computer.org/csdl/proceedings/cloud/2018/7235/00/723501a383-abs.html) shows how
Twister2 performs against Apache Spark and MPI. Please note that Spark KMeans example is written using the data level API 
while Twister2 and MPI implementations are communication level applications. However it is clear that Twister2 performs on the same
level as OpenMPI which is an highly optimized communication library in the HPC world. And it out performs Spark by roughly a factor of x10.

Notation :   
`DFW` refers to Twister2  
`BSP` refers to MPI (OpenMPI)  

![Kmeans Performance Comparison](images/kmeans_comparison.png =250x100)
## Things we are working on

These are things we are actively working on and planning to work on.

1. Hierarchical task scheduling - Ability to run different types of jobs in a single dataflow
2. Fault tolerance
3. Data API including DataSet similar to Spark RDD, Flink DataSet and Heron Streamlet
3. Supporting different API's including Storm, Spark, Beam  
4. Heterogeneous resources allocations
5. Web UI for monitoring Twister2 Jobs
6. More resource managers - Pilot Jobs, Yarn
7. More example applications

## Important Links

Harp is a separate project and its documentation can be found in [website](https://dsc-spidal.github.io/harp/)

We use OpenMPI for HP communications [OpenMPI](https://www.open-mpi.org/)
  
Twister2 started as a research project at Indiana University [Digital Science Center](https://www.dsc.soic.indiana.edu/).