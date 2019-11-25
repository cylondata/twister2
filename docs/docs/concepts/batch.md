---
id: batch_jobs
title: Batch Jobs
sidebar_label: Batch Jobs
---

A batch job can be created using three APIs. One can combine these APIs to write portions of their program as well.

These APIs are called

1. TSet API
2. Compute API
3. Operator API

TSet API is similar to Spark RDD, Flink or Beam APIs. Compute API is similar to Hadoop API and requires a deeper 
understanding from the user. Operator API is similar to MPI where only bare minimum functions are provided to the
user.

This document will cover the fundamentals of these APIs and how to create an application using them. 
These APIs are explained in separate documents in detail.

# TSet API

This is the easiest API to develop a batch application. This API represents a batch application as a 
set of data transformations.

An application is written as a collection of TSet transformations. These transformations are converted 
to graphs and executed by the Twister2 runtime. 

Every TSet transformation has a data source, a collection of user defined functions, a set of links and a terminating
action. TSet is evaluated only when there is an action or when user explicitly ask the runtime to 
evaluate the TSet.

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

Now lets look at how to combine two TSet transformations. Such combinations are needed when implementing 
iterations or reading from multiple data sources.

The cache operation or persist operation can be used to save a calculation of a TSet transformation into memory or disk.  

```java

```




