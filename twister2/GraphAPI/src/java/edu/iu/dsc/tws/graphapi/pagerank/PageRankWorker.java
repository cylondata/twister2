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

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at

package edu.iu.dsc.tws.graphapi.pagerank;
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
import edu.iu.dsc.tws.task.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;
import edu.iu.dsc.tws.task.impl.function.ReduceFn;




public class PageRankWorker extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(PageRankWorker.class.getName());

  private static int graphsize = 0;

//  private static double danglingNodeValues;


  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    PageRankWorkerParameters pageRankWorkerParameters = PageRankWorkerParameters.build(config);

    int parallelismValue = pageRankWorkerParameters.getParallelismValue();
    int dsize = pageRankWorkerParameters.getDsize();
    String dataDirectory = pageRankWorkerParameters.getDatapointDirectory();
    int iterations = pageRankWorkerParameters.getIterations();
    graphsize = dsize;

    /* First Graph to partition and read the partitioned data points **/
    ComputeGraph datapointsTaskGraph = buildDataPointsTG(dataDirectory, dsize,
        parallelismValue, config);
    //Get the execution plan for the first task graph
    ExecutionPlan executionPlan = taskExecutor.plan(datapointsTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(datapointsTaskGraph, executionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> graphPartitionData = taskExecutor.getOutput(
        datapointsTaskGraph, executionPlan, "Graphdatasink");

    /* the out of the first graph would like below
    * task Id: 0
    {1=[3, 4], 2=[3, 4, 5]}*/


//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    //the second task graph for assign initial pagerank values for vertex.

    ComputeGraph graphInitialValueTaskGraph = buildGraphInitialValueTG(dataDirectory, dsize,
        parallelismValue, config);
    //Get the execution plan for the first task graph
    ExecutionPlan executionPlan1 = taskExecutor.plan(graphInitialValueTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(graphInitialValueTaskGraph, executionPlan1);
    //Retrieve the output of the first task graph
    DataObject<Object> graphInitialPagerankValue = taskExecutor.getOutput(
        graphInitialValueTaskGraph, executionPlan1, "pageRankValueHolderSink");


    /* the output of second graph should like below
      initiate the pagerank value
    * {1=0.25, 2=0.25}
     */


//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


    long startime = System.currentTimeMillis();
    //third task graph for computations
    ComputeGraph pageranktaskgraph = buildComputationTG(parallelismValue, config);


    ExecutionPlan plan = taskExecutor.plan(pageranktaskgraph);
    //Perform the iterations from 0 to 'n' number of iterations
    for (int i = 0; i < iterations; i++) {
      taskExecutor.addInput(pageranktaskgraph, plan,
          "pageranksource", "graphData", graphPartitionData);

      taskExecutor.addInput(pageranktaskgraph, plan,
          "pageranksource", "graphInitialPagerankValue", graphInitialPagerankValue);

      taskExecutor.itrExecute(pageranktaskgraph, plan);


      graphInitialPagerankValue = taskExecutor.getOutput(pageranktaskgraph, plan,
          "pageranksink");

    }
    taskExecutor.closeExecution(pageranktaskgraph, plan);
    long endTime = System.currentTimeMillis();




    if (workerId == 0) {
      DataPartition<?> finaloutput = graphInitialPagerankValue.getPartition(workerId);
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
    DataObjectSource dataObjectSource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    DataObjectCompute dataObjectCompute = new DataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue);
    DataObjectSink dataObjectSink = new DataObjectSink();

    ComputeGraphBuilder datapointsTaskGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsTaskGraphBuilder.addSource("Graphdatasource", dataObjectSource,
        parallelismValue);
    ComputeConnection datapointComputeConnection = datapointsTaskGraphBuilder.addCompute(
        "Graphdatacompute", dataObjectCompute, parallelismValue);
    ComputeConnection firstGraphComputeConnection = datapointsTaskGraphBuilder.addSink(
        "Graphdatasink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    datapointComputeConnection.direct("Graphdatasource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    firstGraphComputeConnection.direct("Graphdatacompute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    datapointsTaskGraphBuilder.setMode(OperationMode.BATCH);

    datapointsTaskGraphBuilder.setTaskGraphName("datapointsTG");

    //Build the first taskgraph
    return datapointsTaskGraphBuilder.build();
  }

  public  static ComputeGraph buildGraphInitialValueTG(String dataDirectory, int dsize,
                                                       int parallelismValue,
                                                       Config conf) {
    DataObjectSource pageRankValueHolder = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
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
        "pageRankValueHolderSink", pageRankValueHolderSink, parallelismValue);

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

  public static ComputeGraph buildComputationTG(int parallelismValue, Config conf) {

    PageRankSource pageRankSource = new PageRankSource();
    PageRankKeyedReduce pageRankKeyedReduce = new PageRankKeyedReduce();
    PagerankSink pagerankSink = new PagerankSink();

    ComputeGraphBuilder pagerankComputationTaskGraphBuilder = ComputeGraphBuilder.newBuilder(conf);

    pagerankComputationTaskGraphBuilder.addSource("pageranksource",
        pageRankSource, parallelismValue);

    ComputeConnection computeConnectionKeyedReduce = pagerankComputationTaskGraphBuilder.addCompute(
        "pagerankcompute", pageRankKeyedReduce, parallelismValue);

    ComputeConnection computeConnectionAllReduce = pagerankComputationTaskGraphBuilder.addSink(
        "pageranksink", pagerankSink, parallelismValue);

    computeConnectionKeyedReduce.keyedReduce("pageranksource")
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


  private static class PageRankSource extends BaseSource implements Receptor {
    private HashMap<String, ArrayList<String>> graphData;
    private HashMap<String, Double> graphPageRankValue;
    private DataObject<?> graphObject = null;
    private DataObject<?> graphObjectvalues = null;

    private int count = 0;
    private double danglingValueLocal;




    @Override
    public void execute() {
      System.out.println("before call this method");
      DataPartition<?> dataPartition = graphObject.getPartition(context.taskIndex());
      graphData = (HashMap<String, ArrayList<String>>) dataPartition.getConsumer().next();

      DataPartition<?> centroidPartition = graphObjectvalues.getPartition(context.taskIndex());
      graphPageRankValue = (HashMap<String, Double>) centroidPartition.getConsumer().next();

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
                for (int j = 0; j < arrayList.size(); j++) {
                  Double newvalue = valueAndDanglingValue / arrayList.size();
                  context.write("keyedreduce", arrayList.get(j), new double[]{newvalue});

                }

              } else {
                valueAndDanglingValue = (((double) 1 / graphsize) * 0.15)
                    + ((0.85 * recievedDanglingvalue) / graphsize);
                if (arrayList.size() != 0) {
                  for (int j = 0; j < arrayList.size(); j++) {
                    Double newvalue = valueAndDanglingValue / arrayList.size();
                    context.write("keyedreduce", arrayList.get(j), new double[]{newvalue});
                  }

                }
              }
            } else {
              if (value != null) {
                valueAndDanglingValue = value;
                for (int j = 0; j < arrayList.size(); j++) {
                  Double newvalue = valueAndDanglingValue / arrayList.size();
                  context.write("keyedreduce", arrayList.get(j), new double[]{newvalue});

                }

              } else {
                valueAndDanglingValue = ((double) 1 / graphsize) * 0.15;
                if (arrayList.size() != 0) {
                  for (int j = 0; j < arrayList.size(); j++) {
                    Double newvalue = valueAndDanglingValue / arrayList.size();
                    context.write("keyedreduce", arrayList.get(j), new double[]{newvalue});
                  }

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
        context.writeEnd("keyedreduce", "danglingvalues", new double[]{danglingValueLocal});

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



  private static class PageRankKeyedReduce extends BaseCompute {
    private HashMap<String, Double> output = new HashMap<String, Double>();


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
              double pagerankValue  = (0.15 / graphsize) + (0.85 * value);

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
