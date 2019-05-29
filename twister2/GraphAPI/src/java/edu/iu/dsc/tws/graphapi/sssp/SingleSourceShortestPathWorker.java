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
package edu.iu.dsc.tws.graphapi.sssp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.graphapi.vertex.SsspVertex;
import edu.iu.dsc.tws.graphapi.vertex.SsspVertexStatus;
import edu.iu.dsc.tws.task.api.BaseCompute;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class SingleSourceShortestPathWorker extends TaskWorker {
  private static final Logger LOG = Logger.
      getLogger(SingleSourceShortestPathWorker.class.getName());

  private static boolean globaliterationStatus = true;
  private  static String sourceVertexGlobal = null;

  @Override
  public void execute() {

    SsspParameters ssspParameters = SsspParameters.build(config);
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);

    int parallelismValue = ssspParameters.getParallelismValue();
    int dsize = ssspParameters.getDsize();
    String dataDirectory = ssspParameters.getDatapointDirectory();
    String soruceVertex = ssspParameters.getSourcevertex();
    sourceVertexGlobal = soruceVertex;


    /* First Graph to partition and read the partitioned adjacency list datas **/
    DataObjectSource dataObjectSource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    GraphDataCompute graphDataCompute = new GraphDataCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue, soruceVertex);
    GraphDataSink graphDataSink = new GraphDataSink();



    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    taskGraphBuilder.addSource("Graphdatasource", dataObjectSource, parallelismValue);
    ComputeConnection datapointComputeConnection = taskGraphBuilder.addCompute("Graphdatacompute",
        graphDataCompute, parallelismValue);
    ComputeConnection firstGraphComputeConnection = taskGraphBuilder.addSink("Graphdatasink",
        graphDataSink, parallelismValue);



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

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    //the second task graph for assign initial pagerank values for vertex.
    DataObjectSource ssspInitialDatasource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    SsspInitialCompute ssspInitialCompute = new SsspInitialCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue, soruceVertex);
    SsspInitialSink ssspInitialSink = new SsspInitialSink();

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    taskGraphBuilder.addSource("ssspInitialDatasource", ssspInitialDatasource, parallelismValue);
    ComputeConnection dataValue = taskGraphBuilder.addCompute("ssspInitialCompute",
        ssspInitialCompute, parallelismValue);
    ComputeConnection valuecomputeconnection = taskGraphBuilder.addSink("ssspInitialSink",
        ssspInitialSink, parallelismValue);



    //Creating the communication edges between the tasks for the second task graph
    dataValue.direct("ssspInitialDatasource", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    valuecomputeconnection.direct("ssspInitialCompute", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);



    //Build the second taskgraph
    DataFlowTaskGraph graphInitialValueTaskGraph = taskGraphBuilder.build();
    //Get the execution plan for the second task graph
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(graphInitialValueTaskGraph);
    //Actual execution for the second taskgraph
    taskExecutor.execute(graphInitialValueTaskGraph, secondGraphExecutionPlan);
    //Retrieve the output of the second task graph
    DataObject<Object> graphInitialValue = taskExecutor.getOutput(
        graphInitialValueTaskGraph, secondGraphExecutionPlan, "ssspInitialSink");




//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++



    SsspSource ssspSource = new SsspSource();
    SsspKeyedReduce ssspKeyedReduce = new SsspKeyedReduce();
    SsspSink ssspSink = new SsspSink();

    taskGraphBuilder.addSource("ssspSource", ssspSource, parallelismValue);

    ComputeConnection computeConnectionKeyedReduce = taskGraphBuilder.addCompute(
        "ssspKeyedReduce", ssspKeyedReduce, parallelismValue);

    ComputeConnection computeConnectionAllReduce = taskGraphBuilder.addSink(
        "ssspSink", ssspSink, parallelismValue);

    computeConnectionKeyedReduce.keyedReduce("ssspSource",
        "keyedreduce", new Keyedreducefun(), DataType.OBJECT, DataType.INTEGER);

    computeConnectionAllReduce.allreduce("ssspKeyedReduce",
        "all-reduce", new AggregateFn(), DataType.OBJECT);

    taskGraphBuilder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph sssptaskgraph = taskGraphBuilder.build();

    int itr = 0;
    while (globaliterationStatus)  {

      ExecutionPlan plan = taskExecutor.plan(sssptaskgraph);


      taskExecutor.addInput(sssptaskgraph, plan,
          "ssspSource", "graphData", graphPartitionData);

      taskExecutor.addInput(sssptaskgraph, plan,
          "ssspSource", "graphInitialValue", graphInitialValue);
      taskExecutor.execute(sssptaskgraph, plan);

      graphInitialValue = taskExecutor.getOutput(sssptaskgraph, plan,
          "ssspSink");
      itr++;

    }

    System.out.println("Tatol iteration: " + itr);





  }

  private static class SsspSource extends BaseSource implements Receptor  {

    private HashMap<String, SsspVertex> graphData;
    private HashMap<String, SsspVertexStatus> graphSsspStatus;
    private DataObject<?> graphObject = null;
    private DataObject<?> graphObjectvalues = null;

    private int count = 0;
    @Override
    public void execute() {

      DataPartition<?> dataPartition = graphObject.getPartitions(context.taskIndex());
      graphData = (HashMap<String, SsspVertex>) dataPartition.getConsumer().next();

      DataPartition<?> centroidPartition = graphObjectvalues.getPartitions(context.taskIndex());
      graphSsspStatus = (HashMap<String, SsspVertexStatus>) centroidPartition.getConsumer().next();

      if (graphSsspStatus != null) {

        if (count < graphData.size()) {

          for (int i = 0; i < graphData.size(); i++) {
            Object key = graphData.keySet().toArray()[i];
            SsspVertex ssspVertex = graphData.get(key);
            SsspVertexStatus ssspVertexStatus = graphSsspStatus.get(key);
            if (ssspVertexStatus != null) {

              if (ssspVertexStatus.getValue() != Integer.MAX_VALUE) {
                if (ssspVertex.getId().equals(sourceVertexGlobal)) {
                  HashMap<String, Integer> hashMap = ssspVertex.getHashMap();
                  for (int j = 0; j < hashMap.size(); j++) {
                    Object key1 = hashMap.keySet().toArray()[j];
                    context.write("keyedreduce", key1, new int[]{hashMap.get(key1)});
                  }

                  ssspVertex.setStatus(true);
                } else {
                  if (ssspVertex.getValue() > ssspVertexStatus.getValue()) {
                    ssspVertex.setValue(ssspVertexStatus.getValue());
                    HashMap<String, Integer> hashMap = ssspVertex.getHashMap();
                    for (int j = 0; j < hashMap.size(); j++) {
                      Object key1 = hashMap.keySet().toArray()[j];

                      context.write("keyedreduce", key1, new int[]
                          {hashMap.get(key1) + ssspVertex.getValue()});
                    }

                    ssspVertex.setStatus(true);
                  }

                }
              }

            }
            count++;
          }
        } else {
          context.writeEnd("keyedreduce", "taskend", new int[]{10});
        }
      } else {
        globaliterationStatus = false;
        Iterator it = graphData.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry pair = (Map.Entry) it.next();
          String key = (String) pair.getKey();
          SsspVertex ssspVertex = (SsspVertex) pair.getValue();
          System.out.println("vertex: " + key + "value: " + ssspVertex.getValue());

        }
        context.end("keyedreduce");
      }

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

  private static class SsspKeyedReduce extends BaseCompute {
    private HashMap<String, SsspVertexStatus> output = new HashMap<String, SsspVertexStatus>();

    @Override
    public boolean execute(IMessage content) {
      Iterator<Object> it;
      if (content.getContent() instanceof Iterator) {
        it = (Iterator<Object>) content.getContent();

        while (it.hasNext()) {
          Object next = it.next();
          if (next instanceof Tuple) {
            Tuple kc = (Tuple) next;
            if (!kc.getKey().equals("taskend")) {

              SsspVertexStatus ssspVertexStatus = new SsspVertexStatus();
              ssspVertexStatus.setId((String) kc.getKey());
              ssspVertexStatus.setValue(((int[]) kc.getValue())[0]);

              output.put((String) kc.getKey(), ssspVertexStatus);
              context.write("all-reduce", output);
            }


          }
        }
      }

      context.end("all-reduce");
      return true;
    }
  }

  private static class SsspSink extends BaseSink implements Collector {
    private DataObject<Object> datapoints = null;
    private HashMap<String, SsspVertexStatus> finalout = new HashMap<String, SsspVertexStatus>();


    @Override
    public boolean execute(IMessage content) {
      finalout = (HashMap<String, SsspVertexStatus>) content.getContent();
      return true;
    }

    @Override
    public DataPartition<HashMap<String, SsspVertexStatus>> get() {
      return new EntityPartition<>(context.taskIndex(), finalout);
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
      this.datapoints = new DataObjectImpl<>(config);
    }
  }


  private static class Keyedreducefun implements IFunction {

    @Override
    public Object onMessage(Object object1, Object object2) {
      List<Integer> list = new ArrayList<Integer>();
      int[] data1 = (int[]) object1;
      int[] data2 = (int[]) object2;

      list.add(data1[0]);
      list.add(data2[0]);
      int x = list.indexOf(Collections.min(list));
      return new int[]{list.get(x)};
    }
  }

  private static class AggregateFn implements IFunction {

    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {
      HashMap<String, SsspVertexStatus> newout = new HashMap<String, SsspVertexStatus>();
      HashMap<String, SsspVertexStatus> obj1 = (HashMap<String, SsspVertexStatus>) object1;
      HashMap<String, SsspVertexStatus> obj2 = (HashMap<String, SsspVertexStatus>) object2;


      newout.putAll(obj1);
      newout.putAll(obj2);
      return newout;


    }
  }
}
