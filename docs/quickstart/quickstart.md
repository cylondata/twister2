# First steps with Twister2

Lets look at how to setup Twister2 and run few examples. Twister2 is designed
as in incremental distributed system to make it adaptable to different environments.
Lets start with the simplest possible Twister2 job where we spawn set of workers
and print a log.

## Starting parallel workers

At the base of Twister2 is a resource manager that allocates resources for jobs.
Unlike many big data projects that mixes all sorts of capabilities here,
Twister2 resource manager only allocate resources and spawn set of parallel
processes. It is upto the user to do anything with those parallel processes after
they are spawned.

Okay lets exactly that and see how it works. There is a example called HelloWorld.java
included with Twister2 examples package. Note that it implements the IWorker interface,
which is the entry point to any Twister2 job. In the main method of this class
we submit a job to Twister2 with this HelloWorld class as the job class.

```java
package edu.iu.dsc.tws.examples.basic;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

/**
 * This is a Hello World example of Twister2. This is the most basic functionality of Twister2,
 * where it spawns set of parallel workers.
 */
public class HelloWorld implements IWorker {
  private static final Logger LOG = Logger.getLogger(HelloWorld.class.getName());

  @Override
  public void execute(Config config, int workerID,
                      AllocatedResources allocatedResources, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    // lets retrieve the configuration set in the job config
    String helloKeyValue = config.getStringValue("hello-key");

    // lets do a log to indicate we are running
    LOG.log(Level.INFO, String.format("Hello World from Worker %d; there are %d total workers "
            + "and I got a configuration value %s", workerID,
        workerController.getNumberOfWorkers(), helloKeyValue));
  }

  public static void main(String[] args) {
    // lets take number of workers as an command line argument
    int numberOfWorkers = 4;
    if (args.length == 1) {
      numberOfWorkers = Integer.valueOf(args[0]);
    }

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // lets put a configuration here
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("hello-key", "Twister2-Hello");

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setName("hello-world-job")
        .setWorkerClass(HelloWorld.class.getName())
        .setRequestResource(new WorkerComputeResource(2, 1024), numberOfWorkers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
```

Now lets run this class. Lets go inside the twister2 distibution and execute the following command
from twister2 root directory.

```bash
./bin/twister2 submit nodesmpi jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

When this runs it will print a logs like this in to the console.

```bash
[INFO] edu.iu.dsc.tws.examples.basic.HelloWorld: Hello World from Worker 2; there are 8 other workers and I got a configuration value Twister2-Hello
```

It is that simple!

## Communicating between workers

Okay, the next step is to communicate between the workers we have created.
