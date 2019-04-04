# Task API

The Task API is the middle tier API that provides both flexibility and performance. A user directly
models an application as a graph and program it using the Task Graph API.  

```java
  @Override
  public void execute() {
    // source task
    WordSource source = new WordSource();
    // sink task
    WordAggregator counter = new WordAggregator();

    // build the task graph
    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(config);
    // add the source with parallelism of 4
    builder.addSource("word-source", source, 4);
    // add the sink with parallelism of 4 and connect it to source with keyed reduce operator
    builder.addSink("word-aggregator", counter, 4).keyedReduce("word-source", EDGE,
        new ReduceFn(Op.SUM, DataType.INTEGER), DataType.OBJECT, DataType.INTEGER);
    // set the operation mode to batch
    builder.setMode(OperationMode.BATCH);

    // execute the graph
    DataFlowTaskGraph graph = builder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);
    taskExecutor.execute(graph, plan);
  }

```

