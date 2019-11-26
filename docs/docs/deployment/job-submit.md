---
id: job-submit
title: Job Submit
sidebar_label: Job Submit
---

One of our primary goals is to make running Twister2 jobs as simple as possible. 
If you downloaded a twister2 distribution, you just need to unpack that tar file.

If you compiled the source code, following tar package is generated under twister2 directory:
```text
bazel-bin/scripts/package/twister2-0.4.0.tar.gz
```

Unpack the downloaded or generated tar file: 
```bash
$ tar xf bazel-bin/scripts/package/twister2-0.4.0.tar.gz
```

## Submitting a Job

Twister2 jobs are submitted using the ```twister2``` command. This command is found inside the bin
directory of the distribution.

Here is a description of the command

```bash
twister2 submit cluster-type job-type job-file-name job-class-name [job-args]
```

* submit: the command to execute
* cluster-type: either of standalone, kubernetes, nomad, slurm, mesos
* job-type: at the moment, we only support jar
* job-file-name: the file path of the job file (the jar file)
* job-class-name: name of the job class with a main method to execute

Here is an example command:

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 80 -workers 4 -size 1000 -op "allgather" -stages 8,1
```

## Terminating a job

To kill a job, ```twister2 kill`` command is used.

Here is a description of the command:

```bash
twister2 kill cluster-type <job-name>
```

* kill: the command to execute
* cluster-type: either of standalone, kubernetes, nomad, slurm, mesos
* job-name: name of the job to kill

Note: when jobs complete, they are terminated automatically. 
Kill command is used to kill jobs that does not terminate. 
