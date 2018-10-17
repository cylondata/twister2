# Twister2

[![Build Status](https://travis-ci.org/DSC-SPIDAL/twister2.svg?branch=master)](https://travis-ci.org/DSC-SPIDAL/twister2)

Twister2 is a big data tool kit supporting different parallel computing 
paradigms for big data applications. Twister2 models a parallel computation as a graph. 
Depending on the requirements the graph can be deployed, executed with different choices making 
Twister2 capable of running different applications including classic parallel computing, 
data parallel, streaming or function as a service applications.

The detailed desing of Twister2 can be found at

[Twister2 Design](http://dsc.soic.indiana.edu/publications/twister2_design_big_data_toolkit.pdf)

### Contributing to the project

We are an open community that welcomes contributions to the project in any form. 

We use Jira for tracking the progress of twister2.

[Jira](https://twister2.atlassian.net)

The google groups mailing list can be used to ask questions and discussions "twister2@googlegroups.com"

### Twister2 Architecture

Goal of Twister2 is to provide a layered approach for big data with independent components at each 
level to compose an application. The layers include: 1. Resource allocations, 2. Data Access, 
3. Communication, 4. Task System, and 5. Distributed Data. Among these communications, task system 
and data management are the core components of the system with the others providing auxiliary 
services. On top of these layers, one can develop higher-level APIs such as SQL interfaces. 
The following figure shows the runtime architecture of Twister2 with various components. 
Even though shows all the components in a single diagram, one can mix and match various components 
according to their needs. Fault tolerance and security are two aspects that affect all these 
components.

![Alt text](docs/documents/img/tws_architecture.png?raw=true "Title")

The following table gives a summary of various components, APIs, and implementation choices.

![Alt text](docs/documents/img/twister2_architecture.png?raw=true "Title")

