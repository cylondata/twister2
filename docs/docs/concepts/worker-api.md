---
id: worker_api
title: Worker API
sidebar_label: Worker API
---

This is the API that should be implemented by users. Once a Twister2 job is submitted Twister2Worker instances are
created in the cluster. Number of worker instances are specified by the user when submitting the job.

## Twister2Worker Interface

```java
public interface Twister2Worker {

  /**
   * This is the main point of entry for Twister2 jobs.
   * Every job should implement this interface.
   * When a job is submitted, a class implementing this interface gets instantiated
   * and executed by Twister2.
   *
   * As the first thing in the execute method,
   * users are expected to initialize the proper environment object:
   *   for batch jobs: BatchTSetEnvironment
   *   for streaming jobs: StreamingTSetEnvironment
   */
  void execute(WorkerEnvironment workerEnv);
}
```

Users write their programs by implementing the above interface. This interface provides access to the
Twister2 distributed environment. From here onwards a user can use different APIs provided by 
Twister2 to define the application. For example a single application can use [Operator API](operator-api.md), [Task API](task-api.md), 
or [TSet API](tset-api.md) all mixed together to achieve the desired goals.

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