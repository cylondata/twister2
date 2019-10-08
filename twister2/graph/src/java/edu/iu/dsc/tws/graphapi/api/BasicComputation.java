//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.graphapi.api;

import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.Op;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.messaging.types.PrimitiveMessageTypes;
import edu.iu.dsc.tws.api.compute.IFunction;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.compute.nodes.BaseSink;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.graphapi.partition.GraphDataSource;
import edu.iu.dsc.tws.graphapi.vertex.DefaultVertex;
import edu.iu.dsc.tws.graphapi.vertex.VertexStatus;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;
import edu.iu.dsc.tws.task.impl.function.ReduceFn;




public abstract class BasicComputation extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(BasicComputation.class.getName());

  public static int graphsize = 0;
  public static String dataDirectory;
  public static int parallelism = 0;
  public static String sourceVertexGlobal;
  public static boolean globaliterationStatus = true;
  public static int iterations = 0;


  public abstract ComputeGraph computation();
  public abstract ComputeGraph graphInitialization();
  public abstract ComputeGraph graphpartition();


  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    WorkerParameter workerParameter = WorkerParameter.build(config);

    parallelism = workerParameter.getParallelismValue();
    graphsize = workerParameter.getDsize();
    sourceVertexGlobal = workerParameter.getSourcevertex();
    dataDirectory = workerParameter.getDatapointDirectory();
    iterations = workerParameter.getIterations();


    /* First Graph to partition and read the partitioned data points **/
    ComputeGraph graphpartitionTaskGraph = graphpartition();
    //Get the execution plan for the first task graph
    ExecutionPlan executionPlan = taskExecutor.plan(graphpartitionTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(graphpartitionTaskGraph, executionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> graphPartitionData = taskExecutor.getOutput(
        graphpartitionTaskGraph, executionPlan, "GraphPartitionSink");




//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    //the second task graph for assign initial pagerank values for vertex.

    ComputeGraph graphInitializationTaskGraph = graphInitialization();
    //Get the execution plan for the first task graph
    ExecutionPlan executionPlan1 = taskExecutor.plan(graphInitializationTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(graphInitializationTaskGraph, executionPlan1);
    //Retrieve the output of the first task graph
    DataObject<Object> graphInitializationData = taskExecutor.getOutput(
        graphInitializationTaskGraph, executionPlan1, "GraphInitializationSink");




//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


//third task graph for computations
    ComputeGraph computationTaskgraph = computation();
    ExecutionPlan plan = taskExecutor.plan(computationTaskgraph);

    if (iterations != 0) {
      for (int i = 0; i < iterations; i++) {
        taskExecutor.addInput(computationTaskgraph, plan,
            "source", "graphData", graphPartitionData);

        taskExecutor.addInput(computationTaskgraph, plan,
            "source", "graphInitialValue", graphInitializationData);

        taskExecutor.itrExecute(computationTaskgraph, plan);


        graphInitializationData = taskExecutor.getOutput(computationTaskgraph, plan,
            "sink");
      }

      taskExecutor.waitFor(computationTaskgraph, plan);

    } else {
      int itr = 0;
      while (globaliterationStatus) {


        taskExecutor.addInput(computationTaskgraph, plan,
            "source", "graphData", graphPartitionData);

        taskExecutor.addInput(computationTaskgraph, plan,
            "source", "graphInitialValue", graphInitializationData);
        taskExecutor.itrExecute(computationTaskgraph, plan);

        graphInitializationData = taskExecutor.getOutput(computationTaskgraph, plan,
            "sink");
        itr++;

      }
      System.out.println("total Iteration: " + itr);
      taskExecutor.waitFor(computationTaskgraph, plan);
    }



  }


  protected ComputeGraph buildGraphPartitionTG(String path, int dsize,
                                               int parallelismValue,
                                               Config conf,
                                               GraphPartiton graphPartiton) {
    GraphDataSource dataObjectSource = new GraphDataSource(Context.TWISTER2_DIRECT_EDGE,
        path, dsize);

    DataSinkTask dataSinkTask = new DataSinkTask();
    ComputeGraphBuilder graphPartitionTaskGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    graphPartitionTaskGraphBuilder.addSource("Graphdatasource", dataObjectSource,
        parallelismValue);
    ComputeConnection datapointComputeConnection1 = graphPartitionTaskGraphBuilder.addCompute(
        "Graphdatacompute", graphPartiton, parallelismValue);
    ComputeConnection datapointComputeConnection2 = graphPartitionTaskGraphBuilder.addSink(
        "GraphPartitionSink", dataSinkTask, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    datapointComputeConnection1.direct("Graphdatasource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointComputeConnection2.direct("Graphdatacompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    graphPartitionTaskGraphBuilder.setMode(OperationMode.BATCH);

    graphPartitionTaskGraphBuilder.setTaskGraphName("datapointsTG");

    //Build the first taskgraph
    return graphPartitionTaskGraphBuilder.build();
  }

  protected ComputeGraph buildGraphInitialaizationTG(String path, int dsize,
                                                     int parallelismValue,
                                                     Config conf,
                                                     GraphInitialization graphInitialization) {
    GraphDataSource initialaizatioSource = new GraphDataSource(Context.TWISTER2_DIRECT_EDGE,
        path, dsize);
    DataInitializationSinkTask dataInitializationSinkTask = new DataInitializationSinkTask();

    ComputeGraphBuilder initialationTaskGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    initialationTaskGraphBuilder.addSource("initialaizatioSource", initialaizatioSource,
        parallelismValue);
    ComputeConnection datapointComputeConnection = initialationTaskGraphBuilder.addCompute(
        "graphInitializationCompute", graphInitialization, parallelismValue);
    ComputeConnection firstGraphComputeConnection = initialationTaskGraphBuilder.addSink(
        "GraphInitializationSink", dataInitializationSinkTask, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    datapointComputeConnection.direct("initialaizatioSource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    firstGraphComputeConnection.direct("graphInitializationCompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    initialationTaskGraphBuilder.setMode(OperationMode.BATCH);

    initialationTaskGraphBuilder.setTaskGraphName("GraphInitialValueTG");

    //Build the first taskgraph
    return initialationTaskGraphBuilder.build();

  }

  protected ComputeGraph buildComputationTG(int parallelismValue, Config conf,
                                            SourceTask sourceTask,
                                            ComputeTask computeTask,
                                            ReductionFunction reductionFunction,
                                            PrimitiveMessageTypes messageType) {

    SinkTask sinkTask = new SinkTask();

    ComputeGraphBuilder computationTaskGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    computationTaskGraphBuilder.addSource("source",
        sourceTask, parallelismValue);

    ComputeConnection computeConnectionKeyedReduce = computationTaskGraphBuilder.addCompute(
        "compute", computeTask, parallelismValue);

    ComputeConnection computeConnectionAllReduce = computationTaskGraphBuilder.addSink(
        "sink", sinkTask, parallelismValue);

    if (reductionFunction == null) {

      computeConnectionKeyedReduce.keyedReduce("source")
          .viaEdge("keyedreduce")
          .withReductionFunction(new ReduceFn(Op.SUM, messageType))
          .withKeyType(MessageTypes.OBJECT)
          .withDataType(MessageTypes.DOUBLE_ARRAY);
    } else {

      computeConnectionKeyedReduce.keyedReduce("source")
          .viaEdge("keyedreduce")
          .withReductionFunction(reductionFunction)
          .withKeyType(MessageTypes.OBJECT)
          .withDataType(messageType);

    }

    computeConnectionAllReduce.allreduce("compute")
        .viaEdge("all-reduce")
        .withReductionFunction(new Aggregate())
        .withDataType(MessageTypes.OBJECT);

    computationTaskGraphBuilder.setMode(OperationMode.BATCH);
    computationTaskGraphBuilder.setTaskGraphName("buildComputationTG");
    return computationTaskGraphBuilder.build();
  }

  public abstract class GraphPartiton extends ConstructDataStr {


    public GraphPartiton(String edgename, int dsize, int parallel) {
      super(edgename, dsize, parallel);
    }

    public abstract void constructDataStr(IMessage message);
    public void writeMsgOnEdge(String edgeName, HashMap<String, DefaultVertex> hashMap) {
      context.write(edgeName, hashMap);
    }

    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof Iterator) {
        constructDataStr(message);

      }
      context.end(getEdgeName());
      return true;
    }
  }

  public abstract class GraphInitialization extends ConstructDataStr {

    public GraphInitialization(String edgename, int dsize, int parallel) {
      super(edgename, dsize, parallel);
    }

    public abstract void constructDataStr(HashMap<String, VertexStatus> hashMap,
                                          IMessage message);
    public void writeMsgOnEdge(String edgeName, HashMap<String, VertexStatus> hashMap) {
      context.write(edgeName, hashMap);
    }


    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof Iterator) {
        HashMap<String, VertexStatus> map = new HashMap<String, VertexStatus>();
        constructDataStr(map, message);

      }
      context.end(getEdgeName());
      return true;
    }
  }


// abstract source task class

  public abstract class SourceTask extends BaseSource implements Receptor {
    private DataObject<?> graphObject = null;
    private DataObject<?> graphObjectvalues = null;

    public int count = 0;

    public abstract void sendmessage(HashMap<String, DefaultVertex> hashMap1,
                                     HashMap<String, VertexStatus> hashMap2);


    public void writemessage(String edgename, String destinationVertex, double value) {
      context.write(edgename, destinationVertex, new double[]{value});
    }

    @Override
    public void execute() {
      DataPartition<?> dataPartition = graphObject.getPartition(context.taskIndex());
      HashMap<String, DefaultVertex> graphData = (HashMap<String, DefaultVertex>)
          dataPartition.getConsumer().next();

      DataPartition<?> graphInizalationPartition = graphObjectvalues
          .getPartition(context.taskIndex());
      HashMap<String, VertexStatus> graphInitialValue = (HashMap<String, VertexStatus>)
          graphInizalationPartition.getConsumer().next();
      sendmessage(graphData, graphInitialValue);

    }


    @Override
    public void add(String name, DataObject<?> data) {
      if ("graphData".equals(name)) {
        this.graphObject = data;
      }
      if ("graphInitialValue".equals(name)) {
        this.graphObjectvalues = data;
      }
    }
  }

  //abstracted compute task class

  public  abstract class ComputeTask extends BaseCompute {


    public abstract void calculation(Iterator iterator, HashMap<String, VertexStatus> hashMap);


    @Override
    public boolean execute(IMessage content) {
      HashMap<String, VertexStatus> output = new HashMap<String, VertexStatus>();
      Iterator<Object> it;
      if (content.getContent() instanceof Iterator) {
        it = (Iterator<Object>) content.getContent();
        calculation(it, output);

      }
      context.end("all-reduce");
      return true;
    }


    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
    }
  }

  //general Sink class

  private static class SinkTask extends BaseSink implements Collector {
    private DataObject<Object> datapoints = null;
    private HashMap<String, VertexStatus> finalout;


    @Override
    public boolean execute(IMessage content) {
      finalout = (HashMap<String, VertexStatus>) content.getContent();
      return true;
    }

    @Override
    public DataPartition<HashMap<String, VertexStatus>> get() {
      return new EntityPartition<>(context.taskIndex(), finalout);
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      this.datapoints = new DataObjectImpl<>(config);
    }

  }

  //general all-reduce function

  private static class Aggregate implements IFunction {

    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {
      HashMap<String, Double> newout = new HashMap<String, Double>();
      HashMap<String, Double> obj1 = (HashMap<String, Double>) object1;
      HashMap<String, Double> obj2 = (HashMap<String, Double>) object2;


      newout.putAll(obj1);
      newout.putAll(obj2);
      return newout;


    }
  }

  //this for edge function implemnentations like ked reduce function.

  public  abstract class ReductionFunction implements IFunction {

  }



}



