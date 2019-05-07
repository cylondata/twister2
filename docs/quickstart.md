# Quickstart

Lets look at how to setup Twister2 and run few examples. Twister2 is designed as in incremental distributed system to make it adaptable to different environments. Lets start with the simplest possible Twister2 job where we spawn set of workers and print a log.

## Building Twister2

First we need to build Twister2. [Compiling Twister2](compiling/compiling.md) explains how to build it. After building the code and
following the instructions in [Twister2 Distribution](https://twister2.gitbook.io/twister2/compiling/linux#twister2-distribution) you should have a extracted folder named `twister2-0.2.1`, this would be your twister2 home folder.

## Starting parallel workers

At the base of Twister2 is a resource manager that allocates resources for jobs. Unlike many big data projects that mixes all sorts of capabilities here, Twister2 resource manager only allocate resources and spawn set of parallel processes. It is upto the user to do anything with those parallel processes after they are spawned.

Okay lets exactly that and see how it works. There is a example called HelloWorld.java included with Twister2 examples package. Note that it implements the IWorker interface, which is the entry point to any Twister2 job. In the main method of this class we submit a job to Twister2 with this HelloWorld class as the job class.

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

    List<WorkerNetworkInfo> workerList = workerController.waitForAllWorkersToJoin(50000);
    String workersStr = WorkerNetworkInfo.workerListAsString(workerList);
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
        .setName("hello-world-job")
        .setWorkerClass(HelloWorld.class.getName())
        .addComputeResource(2, 1024, numberOfWorkers)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
```

Now lets run this class. Lets go inside the twister2 distribution and execute the following command from twister2 root directory. The Twister2 root directory would be the `twister2-0.2.1` folder that we got during the building of the source code. Go into the
`twister2-0.2.1` directory before executing the commands below.

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.basic.HelloWorld 8
```

When this runs it will print a logs like this in to the console.

```bash
[INFO] edu.iu.dsc.tws.examples.basic.HelloWorld: Hello World from Worker 2; there are 8 other workers and I got a configuration value Twister2-Hello
```

It is that simple!

## Communicating between workers

Okay, the next step is to communicate between the workers we have created. There are many examples in Twister2 that use communication among workers and some of these can be found inside the directory

```bash
examples/src/java/edu/iu/dsc/tws/examples/comms
```

You can run them with a simple command such as

```text
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -op "reduce" -stages 8,1
```

Now, lets focus on a simple communication example where we try to do a word count.

## Word Count Example

Lets look at a word count example. This is a standard example in every other big data system. The code related to example can be found in

```bash
examples/src/java/edu/iu/dsc/tws/examples/batch/wordcount
```

We are using a Keyed Reduce communication operation to calculate the global counts of words, which are emitted from parallel workers.

The example has three main classes.

WordCountWorker that implements the IWorker interface and runs the code.

```java
edu.iu.dsc.tws.examples.batch.wordcount.WordCountWorker
```

BatchWordSource, where we use a thread to generate words and put them into the communication.

```java
edu.iu.dsc.tws.examples.batch.wordcount.BatchWordSouce
```

WordAggregator, where it receives the counts of the words.

```java
edu.iu.dsc.tws.examples.batch.wordcount.WordAggregator
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
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.wordcount.WordCountJob
```

It will output the words and their counts on the console and below is an small sample.

```bash
[INFO] edu.iu.dsc.tws.examples.batch.wordcount.WordAggregator: 12 Word MWf count 83
[INFO] edu.iu.dsc.tws.examples.batch.wordcount.WordAggregator: 12 Word mFu count 105
[INFO] edu.iu.dsc.tws.examples.batch.wordcount.WordAggregator: 12 Word JyDA count 105
```

