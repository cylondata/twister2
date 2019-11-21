---
id: streaming_jobs
title: Streaming Jobs
sidebar_label: Streaming Jobs
---

Twister2 supports streaming jobs to be developed using two APIs.

1. Compute API
2. TSet API

Compute API is similar to the Apache Storm API and TSet API is similar to the Apache Flink API. 
Compute API is more versatile compared to the TSet API and streaming applications are natural fit for this API.
TSet API provides a typed API for streaming.

Streaming job is modeled as a graph which contains sources, computations and links. Every graph starts
with sources and can have multiple computations linked together. The links represent a communication (messaging)
between the sources, and computations.

A user programs an streaming applications by providing implementations of sources, computations and linking them 
together using the communication mechanisms provided by the APIs. 

Lets take the word count example, which is used as a hello world example in big data applications.

![Streaming Word Count](assets/word_count.png)

In this example, set of sources read data from an incoming text stream. A hashed based routing is used to send 
the words to a correct compute task. At this point a global count of a word can be calculated because each word goes to 
its corresponding task every time.

Lets look at how this graph can be created and executed with Twister2.

```java
    ComputeEnvironment cEnv = ComputeEnvironment.init(config, workerID,
        workerController, persistentVolume, volatileVolume);

    // create source and aggregator
    WordSource source = new WordSource();
    WordAggregator counter = new WordAggregator();

    // build the graph
    ComputeGraphBuilder builder = ComputeGraphBuilder.newBuilder(config);
    builder.addSource("word-source", source, 4);
    builder.addCompute("word-aggregator", counter, 4)
        .partition("word-source")
        .viaEdge("aggregate")
        .withDataType(MessageTypes.OBJECT);
    builder.setMode(OperationMode.STREAMING);

    // build the graph
    ComputeGraph graph = builder.build();
    // execute graph
    cEnv.getTaskExecutor().execute(graph);
``` 