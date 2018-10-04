# Slurm

The Slurm mode of deployment is suitable for HPC clusters where Slurm is installed.

## Requirements

The nodes running the jobs must be able to SSH into each other without requiring a password. Also 
Slurm mode uses OpenMPI for running the jobs and cannot use the TCP at the moment.

## Running Jobs

In order to run a job you can use the following command

```bash
twister2 submit slurm job-type job-file-name job-class-name [job-args]
```

Here is an example command

```bash
./bin/twister2 submit slurm jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

In this mode, the job is killed immediately when you terminate the client using ```Ctrl + C```.

## Installing OpenMPI

When you compile Twister2, it builds OpenMPI 3.1.2 version with it. This version is
used by Twister2 for its standalone deployment by default.

You can use your own OpenMPI installation when running the jobs. In order to do that, you
need to change the following parameter found in ```conf/slurm/resource.yaml``` to point to your OpenMPI installation.

```bash
# mpi run file, this assumes a mpirun that is shipped with the product
# change this to just mpirun if you are using a system wide installation of OpenMPI
# or complete path of OpenMPI in case you have something custom
twister2.resource.scheduler.mpi.mpirun.file: "twister2-core/ompi/bin/mpirun"
```

You can follow the [Compiling Guide](../compiling.md) to get instructions on how to install and configure OpenMPI.

## How it works

Slurm uses OpenMPI to start the job. Underneath it uses mpirun command to execute the job. You can change the parameters
of mpirun inside the ```conf/slurm/mpi.sh``` script.