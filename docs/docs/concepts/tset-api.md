---
id: tset_api
title: Data API
sidebar_label: Data API
---

The Data API based on TSet is similar to Spark API, Flink API or Heron Streamlet APIs. It provides a typed 
functional style API for programming a distributed application. The user program is written as a 
set of data transformation steps. 

## Example TSet Program

Here is an example TSet program. As usual we program an ```IWorker``` class. First we create the
```BatchTSetEnvironment``` environment. And the we can create a source, transformations and finally
a sink.

```java
public class ExampleTSet implements IWorker, Serializable {
  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    WorkerEnvironment workerEnv = WorkerEnvironment.init(config, workerID, workerController,
        persistentVolume, volatileVolume);
    BatchTSetEnvironment tSetEnv = TSetEnvironment.initBatch(workerEnv);
    
    SourceTSet<int[]> source = tc.createSource(new TestBaseSource(), 4).setName("Source");
    ReduceTLink<int[]> reduce = source.reduce((t1, t2) -> {
      int[] val = new int[t1.length];
      for (int i = 0; i < t1.length; i++) {
        val[i] = t1[i] + t2[i];
      }
      return val;
    });

    reduce.sink(value -> {
      experimentData.setOutput(value);
      System.out.print("Result : " + Arrays.toString(value));
      try {
        verify(OperationNames.REDUCE);
      } catch (VerificationException e) {
        System.out.print("Exception Message : " + e.getMessage());
      }
      return true;
    });
  }
}
```

TSets are executed lazily. Once an action such as *TSet.cache()* is called, the underlying 
dataflow graph will be created and executed based on the TSet execution chain. 

TSets have two main abstractions
  1. TSet
  2. TLink 

## TSet 
This is the data abstraction which executes an operation on certain chunk of data. 

## TLink 
This is the communication abstraction which links two multiple TSets together. Interesting aspect
 is that we can perform any communication operation supported by the Twister2:Net communication 
 fabric using a TLink. 

## TSet Execution Chain 
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

## Cacheable TSets 
Users can cache data of TSets using the *TSet.cache()* method. This would execute the chain upto 
that TSet and load the results to memory.    

## TSet Environments

User needs to create a ```BatchTSetEnvironment``` or ```StreamingTSetEnvironment``` to create a batch
application or a streaming application. 

Once the environment is created a user can create a ```SourceTSet``` to start the data processing application.

## Batch Operations

Twister2 supports these batch operations.

| Operation | Description  |
| :---      | :--                                         |
| Direct    | A one to one mapping from a TSet to another |
| Reduce    | Reduces a TSet into a single value          |
| AllReduce | Reduces a TSet into a single value and replicate this value |
| Gather    | Gather a distributed set of values |
| AllGther  | Gather a distributed set of values and replicate it |
| Partition | Re-distributes the values |
| Broadcast | Replicate a single value to multiple |
| Keyed-Reduce | Reduce based on a key |
| Keyed-Gather | Gather based on a key |
| Keyed-Partition | Partition based on a key |
| Join      | Inner join with a key |
| Union     | Union of two TSets |

## Stream Operators

| Operation | Description  |
| :---      | :--                                         |
| Direct    | A one to one mapping from a TSet to another |
| Reduce    | Reduces a TSet into a single value          |
| AllReduce | Reduces a TSet into a single value and replicate this value |
| Gather    | Gather a distributed set of values |
| AllGther  | Gather a distributed set of values and replicate it |
| Partition | Re-distributes the values |
| Broadcast | Replicate a single value to multiple |



 

