# Multi Stage Example

Multi stage example refers we have one or more intermediate tasks in between the source and sink 
task. 

## Batch Tasks

```java
private static class GeneratorTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;

    private int count = 0;

    @Override
    public void execute() {
      if (count == 999) {
        if (context.writeEnd("partition-edge", "Hello")) {
          count++;
        }
      } else if (count < 999) {
        if (context.write("partition-edge", "Hello")) {
          count++;
        }
      }
    }
  }

  private static class ReduceTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284791L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      count++;
      LOG.info(String.format("%d %d Reduce received count: %d", context.getWorkerId(),
          context.taskId(), count));
      return true;
    }
  }

  @SuppressWarnings("rawtypes")
  private static class PartitionTask extends BaseStreamCompute {
    private static final long serialVersionUID = -254264903510284798L;

    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof Iterator) {
        Iterator it = (Iterator) message.getContent();
        while (it.hasNext()) {
          count += 1;
          context.write("compute-edge", it.next());
        }
      }
      LOG.info(String.format("%d %d Partition Received count: %d", context.getWorkerId(),
          context.taskId(), count));
      return true;
    }
  }
```

## Stream Task  

```java

private static class GeneratorTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;

    private int count = 0;

    @Override
    public void execute() {
      if (count == 999) {
        if (context.writeEnd("partition-edge", "Hello")) {
          count++;
        }
      } else if (count < 999) {
        if (context.write("partition-edge", "Hello")) {
          count++;
        }
      }
    }
  }

  private static class ReduceTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284791L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      count++;
      LOG.info(String.format("%d %d Reduce received count: %d", context.getWorkerId(),
          context.taskId(), count));
      return true;
    }
  }

  @SuppressWarnings("rawtypes")
  private static class PartitionTask extends BaseStreamCompute {
    private static final long serialVersionUID = -254264903510284798L;

    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof Iterator) {
        Iterator it = (Iterator) message.getContent();
        while (it.hasNext()) {
          count += 1;
          context.write("compute-edge", it.next());
        }
      }
      LOG.info(String.format("%d %d Partition Received count: %d", context.getWorkerId(),
          context.taskId(), count));
      return true;
    }
  }

```




In the multi stage example we have a source task, sink task and we name the intermediate task 
as the compute task which extends the BaseStreamCompute or BaseBatchCompute for streaming applications. There can 
be multiple compute tasks depending on the description of the task. In this one we
generate a stream of data in the source task and in the intermediate task the partition
is done and in the final task we do a reduction operation. 


```java
public void execute() {
    GeneratorTask g = new GeneratorTask();
    ReduceTask rt = new ReduceTask();
    PartitionTask r = new PartitionTask();

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(config);
    builder.addSource("source", g, 4);
    ComputeConnection pc = builder.addCompute("compute", r, 4);
    pc.partition("source", "partition-edge", DataType.OBJECT);
    ComputeConnection rc = builder.addSink("sink", rt, 1);
    rc.reduce("compute", "compute-edge", new IFunction() {
      @Override
      public Object onMessage(Object object1, Object object2) {
        return object1;
      }
    });
    builder.setMode(OperationMode.STREAMING);

    DataFlowTaskGraph graph = builder.build();
    TaskUtils.execute(config, allocatedResources, graph, workerController);
  }
```

Here the ComputeConnection class calls the reduce function to reduce the data via the compute 
task to the sink task. 

Run the following command to run this example. 

## Batch Example

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.batch.MultiStageGraph
```

## Streaming Example

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.streaming.MultiStageGraph
```