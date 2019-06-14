# TSet API

The TSet API is similar to Spark RDD, Flink DataSet or Heron Streamlet APIs. It provides a typed 
functional style API for programming a distributed application. The user program is written as a 
set of data transformation steps. 


```java
  @Override
  public void execute(TwisterBatchContext tc) {
    // set the parallelism of source to task stage 0
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    SourceTSet<int[]> source = tc.createSource(new TestBaseSource(),
        sourceParallelism).setName("Source");
    ReduceTLink<int[]> reduce = source.reduce((t1, t2) -> {
      int[] val = new int[t1.length];
      for (int i = 0; i < t1.length; i++) {
        val[i] = t1[i] + t2[i];
      }
      return val;
    });

    reduce.sink(value -> {
      experimentData.setOutput(value);
      LOG.info("Result : " + Arrays.toString(value));
      try {
        verify(OperationNames.REDUCE);
      } catch (VerificationException e) {
        LOG.info("Exception Message : " + e.getMessage());
      }
      return true;
    });
  }
```

TSets will be executed lazily. Once an action such as *TSet.cache()* is called, the underlying 
dataflow graph will be created and executed based on the TSet execution chain. 

TSets have two main abstractions
  1. TSet
 2. TLink 

## TSet 
This is the data abstraction which executes an operation on certain chunk of data. 

##TLink 
This is the communication abstraction which links two multiple TSets together. Interesting aspect
 is that we can perform any communication operation supported by the Twister2:Net communication 
 fabric using a TLink. 

##TSet Execution Chain 
Users can create a chain of execution using TSets and TLinks. A TSet would expose a set of 
methods which exposes the downstream TLinks and similarly, a TLink would expose a set of methods 
which exposes the TSets which it can connect into. 

Example: 

```java
BatchSourceTSet<> source = tc.createSource(...).setName("Source");
PartitionTLink<> partitioned = source.partition(...);
IterableMapTSet<> mapedPartition = partitioned.map(...);
``` 

Entry point to the execution chain is by creating a Source using the *TwisterBatchContext* or 
*TwisterStreamingContext*. 

##Cacheable TSets 
Users can cache data of TSets using the *TSet.cache()* method. This would execute the chain upto 
that TSet and load the results to memory.    

##Limitations 
Currently TSets do not support branching in the Execution Chain. This will be fixed in the future
 releases. 