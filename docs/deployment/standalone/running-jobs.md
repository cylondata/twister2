# Standalone

The standalone mode of deployment is most easiest way to deploy a Twister2 application.

## Requirements

The nodes running the jobs must be able to SSH into each other without requiring a password.

## Running Jobs

In order to run a job you can use the following command

```bash
twister2 submit standalone job-type job-file-name job-class-name [job-args]
```

Here is an example command

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

In this mode, the job is killed immediately when you terminate the client using ```Ctrl + C```.

## Configurations

You can configure the nodes by editing the ```nodes``` file found under ```conf/standalone/nodes```.

Here enter the the node address and the number of worker you can run on each of them.

By default we ship the following nodes file.

```bash
localhost slots=16
```

Here it says we can run up to 16 workers in the local machine. You can add more machines with their capacity
to this file in order to run the job on them.

## Installing OpenMPI

When you compile Twister2, it builds OpenMPI 3.1.2 version with it. This version is
used by Twister2 for its standalone deployment by default.

You can use your own OpenMPI installation when running the jobs. In order to do that, you
need to change the following parameter found in ```conf/standalone/resource.yaml``` to point to your OpenMPI installation.

```bash
# mpi run file, this assumes a mpirun that is shipped with the product
# change this to just mpirun if you are using a system wide installation of OpenMPI
# or complete path of OpenMPI in case you have something custom
twister2.resource.scheduler.mpi.mpirun.file: "twister2-core/ompi/bin/mpirun"
```

You can follow the [Compiling Guide](../compiling.md) to get instructions on how to install and configure OpenMPI.

## How it works

Standalone uses OpenMPI to start the job. Underneath it uses mpirun command to execute the job. You can change the parameters
of mpirun inside the ```conf/standalone/exp.sh``` script.