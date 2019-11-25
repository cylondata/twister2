---
id: quickstart
title: Quickstart
sidebar_label: Quickstart
---

Lets look at how to setup Twister2 and run few examples. Lets start with a simple Twister2 job to spawn set of parallel workers and print a log.

### Building Twister2

First we need to build Twister2. [Compiling Twister2](compiling/compiling.md) explains how to build it. After building the code and
following the instructions in [Twister2 Distribution](compiling/linux.md) you should have a extracted folder named `twister2-0.4.0`, this would be your twister2 home folder.

### Starting parallel workers

At the base of Twister2 is a resource manager that allocates resources for jobs. Unlike other big data projects that mixes different capabilities here, Twister2 resource manager only allocate resources and spawn set of parallel processes. 

There is an example called HelloWorld.java included with Twister2 examples package. Note that it implements the IWorker interface, which is the entry point to any Twister2 job. In the main method of this class we submit a job to Twister2 with this HelloWorld class as the job class.

```java
package edu.iu.dsc.tws.examples.basic;

import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;

/**
 * This is a Hello World example of Twister2. This is the most basic functionality of Twister2,
 * where it spawns set of parallel workers.
 */
public class HelloWorld implements IWorker {

  private static final Logger LOG = Logger.getLogger(HelloWorld.class.getName());

  @Override
  public void execute(Config config, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    // lets retrieve the configuration set in the job config
    String helloKeyValue = config.getStringValue("hello-key");

    // lets do a log to indicate we are running
    LOG.log(Level.INFO, String.format("Hello World from Worker %d; there are %d total workers "
            + "and I got a message: %s", workerID,
        workerController.getNumberOfWorkers(), helloKeyValue));

    List<JobMasterAPI.WorkerInfo> workerList = null;
    try {
      workerList = workerController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }
    String workersStr = WorkerInfoUtils.workerListAsString(workerList);
    LOG.info("All workers have joined the job. Worker list: \n" + workersStr);

    try {
      LOG.info("I am sleeping for 1 minute and then exiting.");
      Thread.sleep(60 * 1000);
      LOG.info("I am done sleeping. Exiting.");
    } catch (InterruptedException e) {
      LOG.severe("Thread sleep interrupted.");
    }

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
        .setJobName("hello-world-job")
        .setWorkerClass(HelloWorld.class)
        .addComputeResource(2, 1024, numberOfWorkers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
```

Now lets run this class using command line. Lets go inside the twister2 distribution and execute the following command from twister2 root directory. Go into the
`twister2-0.4.0` directory before executing the commands below.

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

When this runs it will print a logs like this in to the console.

```bash
[INFO] edu.iu.dsc.tws.examples.basic.HelloWorld: Hello World from Worker 2; there are 8 other workers and I got a configuration value Twister2-Hello
```

It is that simple!

The above command submits a job class using the standalone mode. The command accepts the jar file containing the main class and the class name to run.

### Communicating between workers

Okay, the next step is to communicate between the workers we have created. There are many examples in Twister2 that use communication among workers and some of these can be found inside the directory

```bash
examples/src/java/edu/iu/dsc/tws/examples/comms
```

You can run them with a simple command such as

```text
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -op "reduce" -stages 8,1 -workers 4
```

Now, lets focus on a simple communication example where we try to do a word count.

### Word Count Example

Lets look at a word count example. This is a standard example in every other big data system. The code related to example can be found in

```bash
examples/src/java/edu/iu/dsc/tws/examples/batch/wordcount/task
```

We are using a Keyed Reduce communication operation to calculate the global counts of words, which are emitted from parallel workers.

The example has three main classes.

WordCountWorker that implements the IWorker interface and runs the code.

```java
edu.iu.dsc.tws.examples.batch.wordcount.task.WordCountWorker
```

BatchWordSource, where we use a thread to generate words and put them into the communication.

```java
edu.iu.dsc.tws.examples.batch.wordcount.task.BatchWordSouce
```

WordAggregator, where it receives the counts of the words.

```java
edu.iu.dsc.tws.examples.batch.wordcount.task.WordAggregator
```

The WordCountWorker sets up communications and task ids. Then it sets up the communication operation.

```java
this.taskPlan = Utils.createStageTaskPlan(
    cfg, resources, taskStages, workerList);

setupTasks();
setupNetwork(workerController, resources);

// create the communication
wordAggregator = new WordAggregator();
keyGather = new BKeyedReduce(channel, taskPlan, sources, destinations,
    new ReduceOperationFunction(Op.SUM, MessageType.INTEGER),
    wordAggregator, MessageType.OBJECT, MessageType.INTEGER, new HashingSelector());
```

We send the messages through this communication operation using the code in BatchWordSource

```java
String word = sampleWords.get(random.nextInt(sampleWords.size()));
// lets try to process if send doesn't succeed
while (!operation.reduce(taskId, word, new int[]{1}, 0)) {
  operation.progress();
}
```

We send 1 as the word count and it will be summed up for the each word.

We receive the final word counts as an iteration in the WordAggregator.

```java
public boolean receive(int target, Iterator<Object> it) {
while (it.hasNext()) {
  Object next = it.next();
  if (next instanceof ImmutablePair) {
    ImmutablePair kc = (ImmutablePair) next;
    LOG.log(Level.INFO, String.format("%d Word %s count %s",
        target, kc.getKey(), ((int[]) kc.getValue())[0]));
  }
}
isDone = true;
return true;
}
```

### Running WordCount

Here is the command to run this example

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.wordcount.task.WordCountJob
```

It will output the words and their counts on the console and below is an small sample.

```bash
[2019-08-22 16:41:22 -0400] [INFO] [worker-3] [main] edu.iu.dsc.tws.examples.batch.wordcount.task.WordCountJob: 100003 Word aw count 1  
[2019-08-22 16:41:22 -0400] [INFO] [worker-3] [main] edu.iu.dsc.tws.examples.batch.wordcount.task.WordCountJob: 100003 Word z count 5  
[2019-08-22 16:41:22 -0400] [INFO] [worker-3] [main] edu.iu.dsc.tws.examples.batch.wordcount.task.WordCountJob: 100003 Word DWm count 3  
[2019-08-22 16:41:22 -0400] [INFO] [worker-3] [main] edu.iu.dsc.tws.examples.batch.wordcount.task.WordCountJob: 100003 Word wU count 1  
[2019-08-22 16:41:22 -0400] [INFO] [worker-3] [main] edu.iu.dsc.tws.examples.batch.wordcount.task.WordCountJob: 100003 Word Ge count 1  
[2019-08-22 16:41:22 -0400] [INFO] [worker-3] [main] edu.iu.dsc.tws.examples.batch.wordcount.task.WordCountJob: 100003 Word yW count 2  
[2019-08-22 16:41:22 -0400] [INFO] [worker-3] [main] edu.iu.dsc.tws.rsched.schedulers.standalone.MPIWorker: Worker finished executing - 3  
[2019-08-22 16:41:22 -0400] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.JobMaster: All 4 workers have completed. JobMaster is stopping.  
```

