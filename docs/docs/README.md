---
id: introduction
title: Overview
sidebar_label: Overview
---

Twister2 is a big data environment for streaming, data processing, and analytics. A user can define 
batch applications or streaming applications with different API's provided by twister2. This is 
possible due to the modular architecture of different components of the framework. These components 
can be mixed to define different application. 

These components include a resource provisioning layer to interface with cluster resource managers, 
parallel communication operator module recognizing the need for both data operators and bulk 
synchronous parallel (BSP) operators, task system for abstracting out computations and data 
representation for data manipulation. 

## Run your first application

First step in running a Twister2 application is to get a source tar and build it. Once you build the
source code, it is just a single command to start an application.

Refer to the [Compilation](compiling/compiling.md) for details on how to compile. 

After you compile twister2 you can go to

```bash
cd $TWISTER_SOURCE/bazel-bin/scripts/package/twister2-VERSION
```

and run following command to start an application

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

It is that easy!

## Why Twister2

Twister2 provides streaming and batch capabilities similar to big data platforms such as Apache Hadoop, Apache Spark,
Apache Flink and Apache Storm in a single platform. Twister2 is built from bottom up to be used in both cloud
and HPC environments making it an ideal candidate for bridging the gap between High performance computing and big data
computing.

Twister2 doesn't use a driver program as in Spark giving instance creation of tasks. Also it can support pure streaming
similar to Apache Storm or Apache Heron. 

Flink is mostly a streaming engine and it uses those capabilities to support batch applications. For example
Flink cannot support nested iterations. Twister2 supports iterations natively and unlike in Spark where iterations happen
in the driver (high overhead) Twister2 iterations happens in the workers.
 

## Software used by Twister2

Harp is a separate project and its documentation can be found in [website](https://dsc-spidal.github.io/harp/)

We use OpenMPI for HP communications [OpenMPI](https://www.open-mpi.org/)
  
Twister2 started as a research project at Indiana University [Digital Science Center](https://www.dsc.soic.indiana.edu/).

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Acknowledgements

This work was partially supported by NSF CIF21 DIBBS 1443054 and the Indiana University Precision Health initiative.
We thank Intel for their support of the Juliet and Victor systems and extend our gratitude to the FutureSystems team for their support with the infrastructure.
