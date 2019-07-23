---
id: op_api
title: Operator API
sidebar_label: Operator API
---

This is the lowest level of API provided by Twister2. It provides the basic parallel operators required
by a parallel programs in terms of both Bulk Synchronous Parallel (BSP) and DataFlow API. 

The BSP APIs are provided by Harp and MPI specification (OpenMPI).

The DataFlow operators are implemented by Twister2 as a Twister:Net library.

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
 