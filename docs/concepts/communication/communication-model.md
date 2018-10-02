# Communication Model

Twister2 supports a dataflow communication model. A dataflow program models a computation as a graph with nodes of the graph doing user-defined computations and edges representing the communication links between the nodes. The data flowing through this graph is termed as events or messages. It is important to note that even though by definition dataflow programming means data is flowing through a graph, it may not necessarily be the case physically, especially in batch applications. Big data systems employ different APIs for creating the dataflow graph. For example, Flink and Spark provide distributed dataset-based APIs for creating the graph while systems such as Storm and Hadoop provide task-level APIs.

We support the following dataflow operations.

1. Reduce
2. Gather
3. AllReduce
4. AllGather
5. Partition
6. Broadcast
7. Keyed Reduce
8. Keyed Partition
9. Keyed Gather

Dataflow communications are overlaid on top of worker processes using logical ids.

We support both streaming and batch versions of these operations.

## TaskPlan

Task plan represents how the IDs are overlyed on top of the workers. A worker process will have a unique ID.

## Streaming

Streaming communication is a continuous flow of data.

## Batch

A batch operation works on a finite amount of data. Hence it terminates after the data sending is completed.

## Shuffle Engine

In case of in-sufficient memory, we can persist the data to disk.

## Communication Operation

A communication operation is defined by, set of source IDs, set of target IDs, a message receiver, a partial message receiver and set of edge IDs.

## Detecting termination

Because we are working on a distributed setting with a communication having multiple steps, termination of the operation needs to be detected.

