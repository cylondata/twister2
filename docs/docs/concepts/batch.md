---
id: batch_jobs
title: Batch Jobs
sidebar_label: Batch Jobs
---

A batch job can be created using three APIs. One can combine these APIs to write portions of their program as well. These APIs are called

* TSet API
* Compute API
* Operator API

TSet API is similar to Spark RDD, Flink or Beam APIs. Compute API is similar to Hadoop API and requires a deeper 
understanding from the user. Operator API is similar to MPI where only bare minimum functions are provided to the
user.

This document will cover the fundamentals of these APIs and how to create an application using them. 
These APIs are explained in separate documents in detail.

## Anatomy of a batch job

Every Twister2 job is executed by set of ```IWorker``` instances. How many such instances are created
is configured when submitting the job. A user directly programs an ```IWorker``` instance. Unlike in 
other data systems that have a central controller for job submission, Twister2 uses fully distributed 
job creation. This means the user program specified in the ```IWorker``` is executed by all the parallel
workers at the same time.

Within the ```IWorker``` program user can create multiple data transformations and run them, connect them using
in-memory or disk based operations. Without a central scheduler, the data transformation and graph creation is
instantaneous in Twister2 paving way to a more efficient data processing system.

![Batch Execution](assets/iworker.png) 

## TSet API

This is the easiest API to develop a batch application. This API represents a batch application as a 
set of data transformations.

An application is written as a collection of TSet transformations. These transformations are converted 
to graphs and executed by the Twister2 runtime. 

Every TSet transformation has a data source, a collection of user defined functions, a set of links and a terminating
action. TSet is evaluated only when there is an action or when user explicitly ask the runtime to 
evaluate the TSet.

### Actions

Here is a simple TSet transformation where we output ```Integer``` values ranging from ```0-10``` from 
a source and printed. In this example source is defined by the user and terminating action is the 
```forEach``` directive. The source is connected to foreach action by an in-memory link called ```direct```.

```java
SourceTSet<Integer> simpleSource = env.createSource(new SourceFunc<Integer>() {
  private int count = 0;
  
  @Override
  public boolean hasNext() {
    return count < 10;
  }
  
  @Override
  public Integer next() {
    return count++;
  }
}, 4);

simpleSource.direct().forEach(i -> {
  System.out.println("i : " + i);
});
```

When an action is specified, the TSet is evaluated at that point.

| Action | Description 
| :--- | :--- | 
| forEach  | Iterate through the elements of a TSet |  
| cache | Cache the elements into memory |

If an action is not specified, a user can explicitly execute the TSet transformation using the ```run()``` method of
the ```TSetEnvironment```.

```java
env.run()
```

### Cached or Persistent TSets and Inputs

Now lets look at how to combine two TSet transformations by first caching a TSet and using it in another transformation.

The cache operation or persist operation can be used to save a calculation of a TSet transformation into memory or disk. 
These saved TSets can be used in other TSet transformation as inputs.   

```java
  // create a cached tset
  CachedTSet<Object> cached = env.createSource(new SourceFunc<Integer>() {
  }, 4).direct().compute((itr, c) -> {
  }).direct().compute((itr, c) -> {
  }).cache();

  // second TSet with input added
  env.createSource(new SourceFunc<Integer>() {
  }, 4).direct().compute(
    new ComputeCollectorFunc<Integer, Iterator<Integer>>() {

      private DataPartitionConsumer<Integer> xValues;

      @Override
      public void prepare(TSetContext context) {
        // get the input
        this.xValues = (DataPartitionConsumer<Integer>) context.getInput("x").getConsumer();
      }

      @Override
      public void compute(Iterator<Integer> zValues, RecordCollector<Integer> output) {
        while (zValues.hasNext()) {
          output.collect(xValues.next() + zValues.next());
        }
      }
      // add input
    }).addInput("x", cached).direct().forEach(i -> {});
```

The ```addInput``` method of can be used to give an cached input to another computation. The cached
input is available throught a ```DataPartitionConsumer``` which gives an iterator.

Data can be persisted to disk for check-pointing purposes or relieving the memory pressure for large computations 
without enough memory.  


