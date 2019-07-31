---
id: worker_api
title: Worker API
sidebar_label: Worker API
---

This is the lowest API supported by Twister2. Once a Twister2 job is submitted a worker instances are
created in the cluster. Number of worker instances are specified by the user when submitting the job.

## IWorker Interface

```java
/**
 * This is the main point of entry for a Twister2 job. Every job should implement this interface.
 * When a job is submitted, a class that implements this interface gets instantiated and executed
 * by Twister2.
 */
public interface IWorker {
  /**
   * Execute with the resources configured
   * @param config configuration
   * @param workerID the worker id
   * @param workerController the worker controller
   * @param persistentVolume information about persistent file system
   * @param volatileVolume information about volatile file system
   */
  void execute(Config config,
               int workerID,
               IWorkerController workerController,
               IPersistentVolume persistentVolume,
               IVolatileVolume volatileVolume);

}
```

A user programs a worker by implementing the above interface. This interface provides access to the
Twister2 distributed environment. From here onwards a user can use different APIs provided by 
Twister2 to define the application. For example a single application can use [Operator API](operator-api.md), [Task API](task-api.md), 
or [TSet API](tset-api.md) all mixed together to achieve the desired goals.

Usually the first thing a user would do is to create the ```WorkerEnvironment```. This environment is the base 
for all other environments.

```java

public class ExampleWorker implements IWorker {
  public void execute(Config config,
                 int workerID,
                 IWorkerController workerController,
                 IPersistentVolume persistentVolume,
                 IVolatileVolume volatileVolume) {
    WorkerEnvironment workerEnv = WorkerEnvironment.init(config, workerID, 
                                                        workerController, persistentVolume,
                                                        volatileVolume);  
  }
}
```

## Creating the Job

The main class of the program needs to create and submit the job with required parameters.

```java
public static void main(String[] args) throws ParseException {
  // first load the configurations from command line and config files
  Config config = ResourceAllocator.loadConfig(new HashMap<>());
  // build JobConfig, we can put job parameters in to the job config and access these
  // inside worker through conffg
  JobConfig jobConfig = new JobConfig();
  jobConfig.put("iterations", 10);

  // now create a twister2 job and submit, we need to set the class name of the IWorker here
  Twister2Job twister2Job = Twister2Job.newBuilder()
      .setJobName("job_name")
      .setWorkerClass(IWorkerClazz)
      .addComputeResource(no_cpus_per_worker, memory_per_worker, no_of_workers)
      .setConfig(jobConfig)
      .build();
  // now submit the job
  Twister2Submitter.submitJob(twister2Job, config);
}
```

## Job Submission 

Once the job is compiled to a Jar file, it can be submmitted using the ```twister2 submit``` command.
More details can be found in the [Job Submit](../deployment/job-submit.md) section.