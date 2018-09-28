# Task Examples

Task examples demonstrate the task API. The task api has the capability of deploying
tasks by abstracting the communication logic to the api users. Basically, the task 
layer does the job of creating the task graph based on the user description and builds
the underlying communication. The task API also supports a thread model which user can
select based on preference. Task examples are in two forms representing batch and stream
mode examples. In these examples we address the collective communication usage with 
Twister2. 

## Twister2 Collective Communications

1. Reduce
2. Allreduce
3. Gather
4. AllGather
5. KeyedReduce
6. KeyedGather
7. Broadcast
8. Partition
9. KeyedPartition

After building the project, you can run the batch mode examples as follows. 

```bash
./twister2-dist/bin/twister2 submit nodesmpi jar twister2-dist/examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr <iterations> -workers <workers> -size <data_size> -op "<operation>" -stages <source_parallelsim>,<sink_parallelism> -<flag> -verify
```

### Running Option Definitions

1. itr : integer number which provides the number of iterations which the application must run
2. workers : Number of workers needed for the application
3. data_size : size of the array that is being passed. In the examples, we generate an array specified by this size
4. op : Collective operation type:
    1. reduce
    2. allreduce
    3. gather
    4. allgather
    5. keyed-reduce
    6. keyed-gather
    7. bcast
    8. partition
    9. keyed-partition
5. stages : has to params 
    1. source_parallelism : depending on the collective communication that you prefer select this number
    2. sink_parallelism : depending on the collective communication that you prefer select this number
    Example : For reduce communication it can be 4,1 as sink has to be a single point of receive in the reduction 
    process and source has more data generating points. 
6. flag : for a batch job this flag must be empty, if it is an streaming job, -stream would state the flag. 
7. verify : -verify flag does the verification of the collective operation. This is an optional parameter. 


## Batch Examples

### Reduce Example

```java  

public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int psource = taskStages.get(0);
    int psink = taskStages.get(1);
    Op operation = Op.SUM;
    DataType dataType = DataType.INTEGER;
    String edge = "edge";
    BaseBatchSource g = new SourceBatchTask(edge);
    BaseBatchSink r = new ReduceSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, psource);
    computeConnection = taskGraphBuilder.addSink(SINK, r, psink);
    computeConnection.reduce(SOURCE, edge, operation, dataType);
    return taskGraphBuilder;
}
```
In the buildTaskGraph method we specify the task graph that we are going to build.
This is a very simple task graph which has a source and a sink task with a user defined
parallelism.  The source task is abstracted from the user in the examples, but in the
BenchTaskWorker class, you can see the SourceBatchTask as the source task for batch examples.

```java
protected static class SourceBatchTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;

    public SourceBatchTask() {

    }

    public SourceBatchTask(String e) {
      this.edge = e;
    }

    @Override
    public void execute() {
      Object val = generateData();
      Object last = generateEmpty();
      if (count == 1) {
        if (context.writeEnd(this.edge, last)) {
          count++;
        }
      } else if (count < 1) {
        if (context.write(this.edge, val)) {
          count++;
        }
      }
    }
  }
```

```java
protected static class ReduceSinkTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      count++;
      if (count % 1 == 0) {
        Object object = message.getContent();
        if (object instanceof int[]) {
          LOG.info("Batch Reduce Message Received : " + Arrays.toString((int[]) object));
        }
      }
      return true;
    }
  }
```
In the ReduceSinkTask , we define the logic for processing the reduced data. In this
case it is a simple print to the console. 

Running a reduction operation on a size of 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1, added with result verification. 

```bash
./twister2-dist/bin/twister2 submit nodesmpi jar twister2-dist/examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.ExampleTaskMain -itr 1 -workers 4 -size 8 -op "reduce" -stages 8,1 -verify

```
[Task based Batch Reduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/BTReduceExample.java)



