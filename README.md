# Twister2

Twister2 is a big data tool kit supporting different parallel computing 
paradigms for big data applications. Twister2 models a parallel computation as a graph. Depending on the requirements the graph can be deployed, executed with different choices making Twister2 capable of running different applications including classic parallel computing, data parallel, streaming or function as a service applications.

The detailed desing of Twister2 can be found at

[Twister2 Desing](http://dsc.soic.indiana.edu/publications/twister2_design_big_data_toolkit.pdf)

We use Jira for tracking the progress of twister2.

[Jira](https://twister2.atlassian.net)

## Compiling

The project uses bazel build system. We are using the 0.30 version of bazel.

### Build commands

Compiling the code

bazel build --config=ubuntu twister2/...

Building the packages

bazel build --config=ubuntu //scripts/package:tarpkgs

## Running

To run an twister2 application use the twister2 command.

