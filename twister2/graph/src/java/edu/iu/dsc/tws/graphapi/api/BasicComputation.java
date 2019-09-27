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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.Op;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
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
import edu.iu.dsc.tws.graphapi.pagerank.DataObjectCompute;
import edu.iu.dsc.tws.graphapi.pagerank.DataObjectSink;
import edu.iu.dsc.tws.graphapi.pagerank.PageRankValueHolderCompute;
import edu.iu.dsc.tws.graphapi.pagerank.PageRankValueHolderSink;
import edu.iu.dsc.tws.graphapi.partition.GraphDataSource;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;
import edu.iu.dsc.tws.task.impl.function.ReduceFn;




public abstract class BasicComputation extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(BasicComputation.class.getName());

  public static int graphsize = 0;
  public int parallelism = 0;


  public abstract ComputeGraph computation();


  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    WorkerParameter workerParameter = WorkerParameter.build(config);

    parallelism = workerParameter.getParallelismValue();
    int dsize = workerParameter.getDsize();
    String dataDirectory = workerParameter.getDatapointDirectory();
    int iterations = workerParameter.getIterations();
    int numberofFiles = workerParameter.getNumFiles();
    String filetype = workerParameter.getFilesystem();
    boolean isshared = workerParameter.isShared();

    graphsize = dsize;

    /* First Graph to partition and read the partitioned data points **/
    ComputeGraph graphpartitionTaskGraph = buildDataPointsTG(dataDirectory, dsize,
        parallelism, config);
    //Get the execution plan for the first task graph
    ExecutionPlan executionPlan = taskExecutor.plan(graphpartitionTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(graphpartitionTaskGraph, executionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> graphPartitionData = taskExecutor.getOutput(
        graphpartitionTaskGraph, executionPlan, "GraphPartitionSink");




//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    //the second task graph for assign initial pagerank values for vertex.

    ComputeGraph graphInitializationTaskGraph = buildGraphInitialValueTG(dataDirectory, dsize,
        parallelism, config);
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
    long startime = System.currentTimeMillis();
    for (int i = 0; i < iterations; i++) {
      taskExecutor.addInput(computationTaskgraph, plan,
          "source", "graphData", graphPartitionData);

      taskExecutor.addInput(computationTaskgraph, plan,
          "source", "graphInitialPagerankValue", graphInitializationData);

      taskExecutor.itrExecute(computationTaskgraph, plan);


      graphInitializationData = taskExecutor.getOutput(computationTaskgraph, plan,
          "pageranksink");
    }

    long endTime = System.currentTimeMillis();
    taskExecutor.waitFor(computationTaskgraph, plan);

    if (workerId == 0) {
      DataPartition<?> finaloutput = graphInitializationData.getPartition(workerId);
      HashMap<String, Double> finalone = (HashMap<String, Double>) finaloutput.getConsumer().next();
      LOG.info("Final output After " + iterations + "iterations ");
      Iterator it = finalone.entrySet().iterator();
      Double recivedFinalDanglingValue = finalone.get("danglingvalues");

      double cummulativepagerankvalue = 0.0;
      int num = 0;
      System.out.println(graphsize);
      while (it.hasNext()) {
        Map.Entry pair = (Map.Entry) it.next();
        if (!pair.getKey().equals("danglingvalues")) {
          double finalPagerankValue = (double) pair.getValue()
              + ((0.85 * recivedFinalDanglingValue) / graphsize);
          System.out.print("Vertex Id: " + pair.getKey());
          System.out.printf(" and it's pagerank value: %.15f \n", finalPagerankValue);

          cummulativepagerankvalue += finalPagerankValue;
          num += 1;
        }
        it.remove(); // avoids a ConcurrentModificationException
      }
      System.out.println(recivedFinalDanglingValue);
      System.out.println(num);
      System.out.println(cummulativepagerankvalue);
      System.out.println(cummulativepagerankvalue
          + ((graphsize - num) * ((((double) 1 / graphsize) * 0.15)
          + (0.85 * (recivedFinalDanglingValue / graphsize)))));

      System.out.println("computation time: " + (endTime - startime));

    }


  }


  public static ComputeGraph buildDataPointsTG(String dataDirectory, int dsize,
                                               int parallelismValue,
                                               Config conf) {
    GraphDataSource dataObjectSource = new GraphDataSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory, dsize);
    DataObjectCompute dataObjectCompute = new DataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue);
    DataObjectSink dataObjectSink = new DataObjectSink();

    ComputeGraphBuilder graphPartitionTaskGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    graphPartitionTaskGraphBuilder.addSource("Graphdatasource", dataObjectSource,
        parallelismValue);
    ComputeConnection datapointComputeConnection1 = graphPartitionTaskGraphBuilder.addCompute(
        "Graphdatacompute", dataObjectCompute, parallelismValue);
    ComputeConnection datapointComputeConnection2 = graphPartitionTaskGraphBuilder.addSink(
        "GraphPartitionSink", dataObjectSink, parallelismValue);

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

  public  static ComputeGraph buildGraphInitialValueTG(String dataDirectory, int dsize,
                                                       int parallelismValue,
                                                       Config conf) {
    GraphDataSource pageRankValueHolder = new GraphDataSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory, dsize);
    PageRankValueHolderCompute pageRankValueHolderCompute = new PageRankValueHolderCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue);
    PageRankValueHolderSink pageRankValueHolderSink = new PageRankValueHolderSink();


    ComputeGraphBuilder pagerankInitialationTaskGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    pagerankInitialationTaskGraphBuilder.addSource("pageRankValueHolder", pageRankValueHolder,
        parallelismValue);
    ComputeConnection datapointComputeConnection = pagerankInitialationTaskGraphBuilder.addCompute(
        "pageRankValueHolderCompute", pageRankValueHolderCompute, parallelismValue);
    ComputeConnection firstGraphComputeConnection = pagerankInitialationTaskGraphBuilder.addSink(
        "GraphInitializationSink", pageRankValueHolderSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    datapointComputeConnection.direct("pageRankValueHolder")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    firstGraphComputeConnection.direct("pageRankValueHolderCompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    pagerankInitialationTaskGraphBuilder.setMode(OperationMode.BATCH);

    pagerankInitialationTaskGraphBuilder.setTaskGraphName("GraphInitialValueTG");

    //Build the first taskgraph
    return pagerankInitialationTaskGraphBuilder.build();

  }

  public  ComputeGraph buildComputationTG(int parallelismValue, Config conf,
                                          PageRankSource pageRankSource,
                                          PageRankKeyedReduce pageRankKeyedReduce) {

    PagerankSink pagerankSink = new PagerankSink();

    ComputeGraphBuilder pagerankComputationTaskGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    pagerankComputationTaskGraphBuilder.addSource("source",
        pageRankSource, parallelismValue);

    ComputeConnection computeConnectionKeyedReduce = pagerankComputationTaskGraphBuilder.addCompute(
        "pagerankcompute", pageRankKeyedReduce, parallelismValue);

    ComputeConnection computeConnectionAllReduce = pagerankComputationTaskGraphBuilder.addSink(
        "pageranksink", pagerankSink, parallelismValue);

    computeConnectionKeyedReduce.keyedReduce("source")
        .viaEdge("keyedreduce")
        .withReductionFunction(new ReduceFn(Op.SUM, MessageTypes.DOUBLE_ARRAY))
        .withKeyType(MessageTypes.OBJECT)
        .withDataType(MessageTypes.DOUBLE_ARRAY);

    computeConnectionAllReduce.allreduce("pagerankcompute")
        .viaEdge("all-reduce")
        .withReductionFunction(new Aggregate())
        .withDataType(MessageTypes.OBJECT);

    pagerankComputationTaskGraphBuilder.setMode(OperationMode.BATCH);
    pagerankComputationTaskGraphBuilder.setTaskGraphName("buildComputationTG");
    return pagerankComputationTaskGraphBuilder.build();
  }




  public abstract class PageRankSource extends BaseSource implements Receptor {
    private DataObject<?> graphObject = null;
    private DataObject<?> graphObjectvalues = null;

    private int count = 0;
    private double danglingValueLocal;



    public abstract void sendmessage(String edgename, ArrayList<String> nearestVertex,
                                     double pageValue);


    public void writemessage(String edgename, String destinationVertex, double value) {
      context.write(edgename, destinationVertex, new double[]{value});
    }

    @Override
    public void execute() {
      DataPartition<?> dataPartition = graphObject.getPartition(context.taskIndex());
      HashMap<String, ArrayList<String>> graphData = (HashMap<String, ArrayList<String>>)
          dataPartition.getConsumer().next();

      DataPartition<?> graphInizalationPartition = graphObjectvalues
          .getPartition(context.taskIndex());
      HashMap<String, Double> graphPageRankValue = (HashMap<String, Double>)
          graphInizalationPartition.getConsumer().next();


      if (count < graphData.size()) {
        for (int i = 0; i < graphData.size(); i++) {
          Object key = graphData.keySet().toArray()[i];
          if (!key.equals("")) {

            Double value = graphPageRankValue.get(key);
            Double recievedDanglingvalue = graphPageRankValue.get("danglingvalues");
            ArrayList<String> arrayList = graphData.get(key);
            Double valueAndDanglingValue = null;

            //when dangling value recived
            if (recievedDanglingvalue != null) {
              if (value != null) {
                valueAndDanglingValue = value + ((0.85 * recievedDanglingvalue) / graphsize);
                sendmessage("keyedreduce", arrayList, valueAndDanglingValue);

              } else {
                valueAndDanglingValue = (((double) 1 / graphsize) * 0.15)
                    + ((0.85 * recievedDanglingvalue) / graphsize);
                if (arrayList.size() != 0) {
                  sendmessage("keyedreduce", arrayList, valueAndDanglingValue);
                }
              }
            } else {
              if (value != null) {
                valueAndDanglingValue = value;
                sendmessage("keyedreduce", arrayList, valueAndDanglingValue);
              } else {
                valueAndDanglingValue = ((double) 1 / graphsize) * 0.15;
                if (arrayList.size() != 0) {
                  sendmessage("keyedreduce", arrayList, valueAndDanglingValue);
                }
              }
            }

            if (arrayList.size() == 0) {

              danglingValueLocal += valueAndDanglingValue;
            }
          }
          count++;


        }
      } else {
        count = 0;
        if (context.writeEnd("keyedreduce", "danglingvalues",
            new double[]{danglingValueLocal})) {
          danglingValueLocal = 0;
        }

      }



    }


    @Override
    public void add(String name, DataObject<?> data) {
      if ("graphData".equals(name)) {
        this.graphObject = data;
      }
      if ("graphInitialPagerankValue".equals(name)) {
        this.graphObjectvalues = data;
      }
    }
  }



  public  abstract class PageRankKeyedReduce extends BaseCompute {
    private HashMap<String, Double> output = new HashMap<String, Double>();

    public abstract double calculation(double value);


    @Override
    public boolean execute(IMessage content) {

      Iterator<Object> it;
      if (content.getContent() instanceof Iterator) {
        it = (Iterator<Object>) content.getContent();

        while (it.hasNext()) {
          Object next = it.next();
          if (next instanceof Tuple) {
            Tuple kc = (Tuple) next;

            if (!kc.getKey().equals("danglingvalues")) {
              double value = ((double[]) kc.getValue())[0];
              double pagerankValue  = calculation(value);

              output.put((String) kc.getKey(), pagerankValue);

              context.write("all-reduce", output);
            } else {
              double danglingValue = ((double[]) kc.getValue())[0];
              output.put((String) kc.getKey(), danglingValue);
              context.write("all-reduce", output);
            }

          }
        }

      }

      context.end("all-reduce");
      return true;
    }


    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
    }
  }

  private static class PagerankSink extends BaseSink implements Collector {
    private DataObject<Object> datapoints = null;
    private HashMap<String, Double> finalout = new HashMap<String, Double>();


    @Override
    public boolean execute(IMessage content) {
      finalout = (HashMap<String, Double>) content.getContent();
      return true;
    }

    @Override
    public DataPartition<HashMap<String, Double>> get() {
      return new EntityPartition<>(context.taskIndex(), finalout);
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      this.datapoints = new DataObjectImpl<>(config);
    }

  }

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



}



