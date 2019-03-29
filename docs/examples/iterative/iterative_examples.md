# Iterative Examples

The iterative task graph computation is mainly useful to perform the iterative computation process 
in the big data world. It generally captures the complex relationship between the entities. In this 
example, we discuss about how to write an iterative example using Twister2 Executor API. Here we 
have a SourceTask which is just doing the generation of data and it is named as IterativeSourceTask 
and the SinkTask which receives the messages is named as the ParitionTask. 


```java

private static class IterativeSourceTask extends BaseBatchSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private DataSet<Object> input;

    private int count = 0;

    @Override
    public void execute() {
      if (count == 999) {
        if (context.writeEnd("partition", "Hello")) {
          count++;
        }
      } else if (count < 999) {
        if (context.write("partition", "Hello")) {
          count++;
        }
      }
    }

    @Override
    public void add(String name, DataSet<Object> data) {
      LOG.log(Level.INFO, "Received input: " + name);
      input = data;
    }
  }

  private static class PartitionTask extends BaseBatchSink implements Collector<Object> {
    private static final long serialVersionUID = -5190777711234234L;

    private List<String> list = new ArrayList<>();

    private int count;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received message: " + message.getContent());

      if (message.getContent() instanceof Iterator) {
        while (((Iterator) message.getContent()).hasNext()) {
          Object ret = ((Iterator) message.getContent()).next();
          count++;
          list.add(ret.toString());
        }
        LOG.info("Message Partition Received : " + message.getContent() + ", Count : " + count);
      }
      count++;
      return true;
    }

    @Override
    public Partition<Object> get() {
      return new Partition<>(context.taskIndex(), list);
    }
  }

```

The iterative logic is defined in the executor method. Here there are 10 iterations
which calls the execute method in the task execution graph. 

```java

public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    IterativeSourceTask g = new IterativeSourceTask();
    PartitionTask r = new PartitionTask();

    TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
    graphBuilder.addSource("source", g, 4);
    ComputeConnection computeConnection = graphBuilder.addSink("sink", r, 4);
    computeConnection.partition("source", "partition", DataType.OBJECT);
    graphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph graph = graphBuilder.build();
    for (int i = 0; i < 10; i++) {
      ExecutionPlan plan = taskExecutor.plan(graph);
      taskExecutor.addInput(graph, plan, "source", "input", new DataSet<>(0));

      // this is a blocking call
      taskExecutor.execute(graph, plan);
      DataSet<Object> dataSet = taskExecutor.getOutput(graph, plan, "sink");
      Set<Object> values = dataSet.getData();
      LOG.log(Level.INFO, "Values: " + values);
    }
  }

```


### To Run Iterative Task Graph Example

./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.batch.IterativeJob


[Iterative Task Graph Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/IterativeJob.java)