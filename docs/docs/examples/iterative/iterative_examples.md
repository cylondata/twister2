---
id: iterative
title: Iterative
sidebar_label: Iterative
---

The iterative task graph computation is mainly useful to perform the iterative computation process 
in the big data world. It generally captures the complex relationship between the entities. In this 
example, we discuss about how to write an iterative example using Twister2 Executor API. Here we 
have a SourceTask which is just doing the generation of data and it is named as IterativeSourceTask 
and the SinkTask which receives the message is named as PartitionTask. 


```java
private static class IterativeSourceTask extends BaseSource implements Receptor {
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

  private static class PartitionTask extends BaseCompute implements Collector {
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
    ComputeConnection computeConnection = graphBuilder.addCompute("sink", r, 4);
    computeConnection.partition("source", "partition", DataType.OBJECT);
    graphBuilder.setMode(OperationMode.BATCH);

    ComputeGraph graph = graphBuilder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);
    IExecutor ex = taskExecutor.createExecution(graph, plan);
    for (int i = 0; i < 10; i++) {
         LOG.info("Starting iteration: " + i);
         // this is a blocking call
         ex.execute();
    }
    ex.closeExecution();
  }
```

### To Run Iterative Task Graph Example

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.task.batch.IterativeJob
```

[Iterative Task Graph Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/task/batch/IterativeJob.java)