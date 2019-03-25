# Storm Compatibility

Twister2 supports the Apache Storm API. One can use a program written in Apache Storm API and run it 
using the Twister2 engine with better performance.

In order to submit a Storm Job a user program can extend from ``Twister2StormWorker.java.`` and create
the StormTopology.

```java

public final class WordCountTopology extends Twister2StormWorker {

  @Override
  public StormTopology buildTopology() {
    int parallelism = config.getIntegerValue("parallelism", 1);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("word", new WordSpout(), parallelism);
    builder.setBolt("consumer", new ConsumerBolt(), parallelism)
        .fieldsGrouping("word", new Fields("word"));

    return builder.createTopology();
  }
}
```

Once this class is programmed they can submit the job using a normal Twister2 Job Submission.

```java

  public static void main(String[] args) {
    int parallelism = 1;

    Config config = ResourceAllocator.loadConfig(
        Collections.emptyMap()
    );

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.put("parallelism", parallelism);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("word-count-storm");
    jobBuilder.setWorkerClass(WordCountTopology.class.getName());
    jobBuilder.setConfig(jobConfig);
    jobBuilder.addComputeResource(1, 512, 1);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
```  

The Job submission command is same as any other Twister2 job.

For more information about Apache Storm API, please refer to its [documentation](http://storm.apache.org/releases/1.2.2/index.html).

