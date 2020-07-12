---
id: started_word_count_source
title: WordCount Source  
sidebar_label: WordCount Source
---

Lets look at the batch word count and streaming word count sources.

### Batch WordCount

The source can be found in [WordCount.java](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/batch/wordcount/tset/WordCount.java).

First lets look at the job creation code.

```java
  /**
   * We submit the job in the main method
   * @param args not using args for this job
   */
public static void main(String[] args) {
  // build JobConfig, these are the parameters of the job
  JobConfig jobConfig = new JobConfig();
  jobConfig.put("NO_OF_SAMPLE_WORDS", 100);
  jobConfig.put("MAX_CHARS", 5);

  Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
  jobBuilder.setJobName("tset-simple-wordcount");
  jobBuilder.setWorkerClass(WordCount.class);
  // we use 2 processes, each with 512mb memory and 1 CPU assigned
  jobBuilder.addComputeResource(1, 512, 2);
  jobBuilder.setConfig(jobConfig);

  // now submit the job
  Twister2Submitter.submitJob(jobBuilder.build(), ResourceAllocator.getDefaultConfig());
}
```

In this code, the job parameters and job resources are specified. ```Twister2JobBuilder``` is used to
build the job. In the above example, 2 parallel processes each with 512 MB of memory is used to run
the example.

```Twister2Submitter.submitJob``` is used to submit the job to the cluster.

#### The Job class

Every Twister2 job should implement ```Twister2Worker``` interface. 
That interface has only one method: execute. 
Inside the execute method, the proper environment object should be initialized. 
In this case, BatchEnvironment is initialized. 

```java
public class WordCount implements Twister2Worker, Serializable {
  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    
  }
}
```

In this method we need to create the computation and execute it.

#### Batch graph

Lets see how the job graph is created and executed.

```java
@Override
public void execute(WorkerEnvironment workerEnv) {
  BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
  int sourcePar = 4;
  Config config = env.getConfig();

  // create a source with fixed number of random words
  SourceTSet<String> source = env.createSource(
      new WordGenerator((int) config.get("NO_OF_SAMPLE_WORDS"), (int) config.get("MAX_CHARS")),
      sourcePar).setName("source");
  // map the words to a tuple, with <word, 1>, 1 is the count
  KeyedTSet<String, Integer> groupedWords = source.mapToTuple(w -> new Tuple<>(w, 1));
  // reduce using the sim operation
  KeyedReduceTLink<String, Integer> keyedReduce = groupedWords.keyedReduce(Integer::sum);
  // print the counts
  keyedReduce.forEach(c -> LOG.info(c.toString()));
}
```

In the above example, we create a ```SourceTSet<String> source```. This source outputs a set of 
words as strings. These strings are mapped to a key, value pair (tuple) using the mapToTuple method.
Finally a key based reduce operation is used to get the global sum of the words.

The ```forEach``` is an action operation, which executes the computation.


### Streaming WordCount

Streaming word count is similar to batch wordcount with few key differences. Job submission is same
in both cases.

#### The Job class

For the job class we implement the ```Twister2Worker``` interface. Inside the execute method, 
we initialize streaming environment object. 

```java
public class WordCount implements Twister2Worker, Serializable {

  @Override
  public void execute(WorkerEnvironment workerEnvironment) {
    StreamingEnvironment cEnv = TSetEnvironment.initStreaming(workerEnvironment);
  }
}
```

#### Streaming graph

Here is the full code of the streaming graph. First we create a source that has an endless output
stream of random words. Then we send those words specific tasks using a hash partitioning. 
Since a given word goes to the same task, we can create a global count of words inside that task.

```java
public void execute(WorkerEnvironment workerEnvironment) {
  StreamingEnvironment cEnv = TSetEnvironment.initStreaming(workerEnvironment);

  // create source and aggregator
  cEnv.createSource(new SourceFunc<String>() {
    // sample words
    private List<String> sampleWords = new ArrayList<>();
    // the random used to pick he words
    private Random random;

    @Override
    public void prepare(TSetContext context) {
      this.random = new Random();
      RandomString randomString = new RandomString(MAX_CHARS, random, RandomString.ALPHANUM);
      for (int i = 0; i < NO_OF_SAMPLE_WORDS; i++) {
        sampleWords.add(randomString.nextRandomSizeString());
      }
    }

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public String next() {
      return sampleWords.get(random.nextInt(sampleWords.size()));
    }
  }, 4).partition(new HashingPartitioner<>()).sink(new SinkFunc<String>() {
    // keep track of the counts
    private Map<String, Integer> counts = new HashMap<>();

    private TSetContext context;

    @Override
    public void prepare(TSetContext context) {
      this.context = context;
    }

    @Override
    public boolean add(String word) {
      int count = 1;
      if (counts.containsKey(word)) {
        count = counts.get(word);
        count++;
      }
      counts.put(word, count);
      LOG.log(Level.INFO, String.format("%d Word %s count %s", context.getIndex(),
          word, count));
      return true;
    }
  });

  // start executing the streaming graph
  cEnv.run();
}
```

