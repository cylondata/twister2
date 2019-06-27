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
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.graphapi.pagerank;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


import edu.iu.dsc.tws.api.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.api.task.function.ReduceFn;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataObject;

import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.BaseCompute;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class PageRankWorker extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(PageRankWorker.class.getName());

  private static int graphsize = 0;

//  private static double danglingNodeValues;


  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    PageRankWorkerParameters pageRankWorkerParameters = PageRankWorkerParameters.build(config);
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);

    int parallelismValue = pageRankWorkerParameters.getParallelismValue();
    int dsize = pageRankWorkerParameters.getDsize();
    String dataDirectory = pageRankWorkerParameters.getDatapointDirectory();
    int iterations = pageRankWorkerParameters.getIterations();
    graphsize = dsize;



    /* First Graph to partition and read the partitioned adjacency list datas **/
    DataObjectSource dataObjectSource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    DataObjectCompute dataObjectCompute = new DataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue);
    DataObjectSink dataObjectSink = new DataObjectSink();



    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    taskGraphBuilder.addSource("Graphdatasource", dataObjectSource, parallelismValue);
    ComputeConnection datapointComputeConnection = taskGraphBuilder.addCompute("Graphdatacompute",
        dataObjectCompute, parallelismValue);
    ComputeConnection firstGraphComputeConnection = taskGraphBuilder.addSink("Graphdatasink",
        dataObjectSink, parallelismValue);



    //Creating the communication edges between the tasks for the second task graph
    datapointComputeConnection.direct("Graphdatasource", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    firstGraphComputeConnection.direct("Graphdatacompute", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);



    //Build the first taskgraph
    DataFlowTaskGraph datapointsTaskGraph = taskGraphBuilder.build();
    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> graphPartitionData = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, "Graphdatasink");

    /* the out of the first graph would like below
    * task Id: 0
    {1=[3, 4], 2=[3, 4, 5]}*/


//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    //the second task graph for assign initial pagerank values for vertex.
    DataObjectSource pageRankValueHolder = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    PageRankValueHolderCompute pageRankValueHolderCompute = new PageRankValueHolderCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue);
    PageRankValueHolderSink pageRankValueHolderSink = new PageRankValueHolderSink();

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    taskGraphBuilder.addSource("pageRankValueHolder", pageRankValueHolder, parallelismValue);
    ComputeConnection dataValue = taskGraphBuilder.addCompute("pageRankValueHolderCompute",
        pageRankValueHolderCompute, parallelismValue);
    ComputeConnection valuecomputeconnection = taskGraphBuilder.addSink("pageRankValueHolderSink",
        pageRankValueHolderSink, parallelismValue);



    //Creating the communication edges between the tasks for the second task graph
    dataValue.direct("pageRankValueHolder", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    valuecomputeconnection.direct("pageRankValueHolderCompute", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);



    //Build the second taskgraph
    DataFlowTaskGraph graphInitialValueTaskGraph = taskGraphBuilder.build();
    //Get the execution plan for the second task graph
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(graphInitialValueTaskGraph);
    //Actual execution for the second taskgraph
    taskExecutor.execute(graphInitialValueTaskGraph, secondGraphExecutionPlan);
    //Retrieve the output of the second task graph
    DataObject<Object> graphInitialPagerankValue = taskExecutor.getOutput(
        graphInitialValueTaskGraph, secondGraphExecutionPlan, "pageRankValueHolderSink");


    /* the output of second graph should like below
      initiate the pagerank value
    * {1=0.25, 2=0.25}
     */


//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++



    //third task graph for computations
    PageRankSource pageRankSource = new PageRankSource();
    PageRankKeyedReduce pageRankKeyedReduce = new PageRankKeyedReduce();
    PagerankSink pagerankSink = new PagerankSink();

    taskGraphBuilder.addSource("pageranksource", pageRankSource, parallelismValue);

    ComputeConnection computeConnectionKeyedReduce = taskGraphBuilder.addCompute(
        "pagerankcompute", pageRankKeyedReduce, parallelismValue);

    ComputeConnection computeConnectionAllReduce = taskGraphBuilder.addSink(
        "pageranksink", pagerankSink, parallelismValue);

    computeConnectionKeyedReduce.keyedReduce("pageranksource",
        "keyedreduce", new ReduceFn(Op.SUM, DataType.DOUBLE), DataType.OBJECT, DataType.DOUBLE);

    computeConnectionAllReduce.allreduce("pagerankcompute",
        "all-reduce", new Aggregate(), DataType.OBJECT);

    taskGraphBuilder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph pageranktaskgraph = taskGraphBuilder.build();





//Perform the iterations from 0 to 'n' number of iterations
    for (int i = 0; i < iterations; i++) {

      /* the out of the first graph would like below
    * task Id: 0
    {1=[3, 4], 2=[3, 4, 5]}*/

      ExecutionPlan plan = taskExecutor.plan(pageranktaskgraph);


      taskExecutor.addInput(pageranktaskgraph, plan,
          "pageranksource", "graphData", graphPartitionData);

      taskExecutor.addInput(pageranktaskgraph, plan,
          "pageranksource", "graphInitialPagerankValue", graphInitialPagerankValue);

     /* the output of second graph should like below
      initiate the pagerank value
    * {1=0.25, 2=0.25}
     */

      taskExecutor.execute(pageranktaskgraph, plan);


      graphInitialPagerankValue = taskExecutor.getOutput(pageranktaskgraph, plan,
          "pageranksink");



    }


    if (workerId == 0) {
      Writer output = null;
      File toptenurllist = new File("/home/anushan/Documents/dinput/output");
      try {
        output = new BufferedWriter(new FileWriter(toptenurllist));
      } catch (IOException e) {
        e.printStackTrace();
      }


      DataPartition<?> finaloutput = graphInitialPagerankValue.getPartitions(workerId);
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

          try {
            output.write("Vertex Id: " + pair.getKey()
                + " and it's pagerank value: "
                + String.format("%.15f", finalPagerankValue) + "\n");

          } catch (IOException e) {
            e.printStackTrace();
          }
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

    }



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

      DataPartition<?> dataPartition = graphObject.getPartitions(context.taskIndex());
      graphData = (HashMap<String, ArrayList<String>>) dataPartition.getConsumer().next();

      DataPartition<?> centroidPartition = graphObjectvalues.getPartitions(context.taskIndex());
      graphPageRankValue = (HashMap<String, Double>) centroidPartition.getConsumer().next();


      if (count < graphData.size()) {
        for (int i = 0; i < graphData.size(); i++) {
          Object key = graphData.keySet().toArray()[i];
          Double value = graphPageRankValue.get(key);
          Double recievedDanglingvalue = graphPageRankValue.get("danglingvalues");
          ArrayList<String> arrayList = graphData.get(key);
          Double valueAndDanglingValue;

          if (recievedDanglingvalue != null) {
            if (value != null) {
              valueAndDanglingValue = value + ((0.85 * recievedDanglingvalue) / graphsize);
            } else {
              valueAndDanglingValue = (((double) 1 / graphsize) * 0.15)
                  + ((0.85 * recievedDanglingvalue) / graphsize);
            }
          } else {
            valueAndDanglingValue = value;
          }

          if (arrayList.size() == 0) {

            danglingValueLocal += valueAndDanglingValue;
          }

          if (value != null) {

            for (int j = 0; j < arrayList.size(); j++) {
              Double newvalue = valueAndDanglingValue / arrayList.size();
              context.write("keyedreduce", arrayList.get(j), new double[]{newvalue});

            }

          } else {

            if (arrayList.size() != 0) {
              for (int j = 0; j < arrayList.size(); j++) {
                Double newvalue = ((((double) 1 / graphsize) * 0.15)
                    + (0.85 * valueAndDanglingValue)) / arrayList.size();
                context.write("keyedreduce", arrayList.get(j), new double[]{newvalue});
              }

            }


          }
          count++;


        }
      } else {
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
