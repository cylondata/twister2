---
id: hello_world
title: Hello Twister2
sidebar_label: Hello Twister2
---

## About this example

With twister2, you get the ability to spawn a set of processes across
a cluster. These processes can be configured to collectively execute a set of instructions on your data based on your data analytics requirement. This example
shows how you can define and submit a twister2 job to execute a logic(for this example, our logic will be juts printing a message to the console) on multiple computing nodes.

## Defining a Twister2 Job

As the first thing, you should define a Twister2 job and provide an entry point to start the execution. You can even declare the required
resources for your job to run, so that twister2 resource scheduling layer can 
provision those resources for your application.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
Job should be defined within the main method of the main class.
```java
public static void main(String[] args) {

    JobConfig jobConfig = new JobConfig();

    Twister2Job job = Twister2Job.newBuilder()
        .setJobName(TSetCheckptExample.class.getName())
        .setConfig(jobConfig)
        .setWorkerClass(TSetCheckptExample.class)
        .addComputeResource(1, 512, 4)
        .build();

    Twister2Submitter.submitJob(job);
}
```

<!--Python-->
In python, job is defined by creating an instance of Twister2Environment. This should be defined in the main python file of the job.

```python
env = Twister2Environment(name="Name is Optional", resources=[{"cpu": 1, "ram": 512, "instances": 4}], config={"Config dictionary is also optional": True})
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Inside twister2 worker

Job definition code run only at the client(machine where you submit the job). Worker code is the logic, that will be executed on multiple machines(nodes) in parallel.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
Your worker class can be extended by BatchTSetIWorker to make it executable inside twister2.
You may or may not, make your main class itself an IWorker.

```java
public class HelloTwister2 implements BatchTSetIWorker {

  private static final Logger LOG = Logger.getLogger(HelloTwister2.class.getName());

  @Override
  public void execute(BatchTSetEnvironment env) {
    LOG.info(String.format("Hello from worker %d", env.getWorkerID()));
  }
}
```

<!--Python-->
In python, whatever you define just after the Twister2Environment environment initialization
will be considered as the worker code.

```python
env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": 4}])

# Your worker code starts here
print("Hello from worker %d" % env.worker_id)
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Submitting the job to twister2

Twister2 accept 4 types of jobs written in two different programing languages.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java Jar-->
A single jar file including the main class and main method.
```bash
./bin/twister2 submit standalone jar <path to jar> <fully qualified class name of main class> <job_args>
```

<!--Java Zip-->
A zip file including the main class and main method.
```bash
./bin/twister2 submit standalone java_zip <path to zip> <fully qualified class name of main class> <job_args>
```

<!--Python File-->
A single python file which creates a Twister2Environment
```bash
./bin/twister2 submit standalone python <path to py file> <job_args>
```

<!--Python Zip-->
A zip file containing multiple python files
```bash
./bin/twister2 submit standalone python_zip <path to zip file> <main python file> <job_args>
```
<!--END_DOCUSAURUS_CODE_TABS-->

