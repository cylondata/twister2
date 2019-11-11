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
package edu.iu.dsc.tws.examples.ml.svm.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.IFunction;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.nodes.BaseCompute;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.data.utils.MLDataObjectConstants;
import edu.iu.dsc.tws.data.utils.WorkerConstants;
import edu.iu.dsc.tws.dataset.partition.EntityPartition;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;


public class SourceTaskDataLoader extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(SourceTaskDataLoader.class.getName());

  private static int workers = 1;
  private static int parallelism = 4;
  private static String dataSource = "";

  @Override
  public void execute() {
    getParams();
    /*
     * First data is loaded from files
     * */
    ComputeGraphBuilder computeGraphBuilder = ComputeGraphBuilder.newBuilder(config);
//    DataObjectSource sourceTask = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
//        dataSource);
//    DataObjectSink sinkTask = new DataObjectSink();
//    computeGraphBuilder.addSource("datapointsource", sourceTask, parallelism);
//    ComputeConnection firstGraphComputeConnection = computeGraphBuilder.addSink(
//        "datapointsink", sinkTask, parallelism);
//    firstGraphComputeConnection.direct("datapointsource",
//        Context.TWISTER2_DIRECT_EDGE, DataType.OBJECT);
//    computeGraphBuilder.setMode(OperationMode.BATCH);
//
//    ComputeGraph datapointsTaskGraph = computeGraphBuilder.build();
//    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
//    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
//    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
//        datapointsTaskGraph, firstGraphExecutionPlan, "datapointsink");
//    LOG.info("Total Partitions : " + dataPointsObject.getPartitions().length);

    /*
     * Second Task
     * */

    DataSourceTask kMeansSourceTask = new DataSourceTask();
    SimpleDataAllReduceTask kMeansAllReduceTask = new SimpleDataAllReduceTask();
    computeGraphBuilder.addSource("kmeanssource", kMeansSourceTask, parallelism);
    ComputeConnection computeConnection = computeGraphBuilder.addCompute(
        "kmeanssink", kMeansAllReduceTask, parallelism);
    computeConnection.allreduce("kmeanssource")
        .viaEdge("all-reduce")
        .withReductionFunction(new SimpleDataAggregator())
        .withDataType(MessageTypes.OBJECT);
    computeGraphBuilder.setMode(OperationMode.BATCH);
    ComputeGraph simpleTaskGraph = computeGraphBuilder.build();
    ExecutionPlan plan = taskExecutor.plan(simpleTaskGraph);
//    taskExecutor.addInput(
//        simpleTaskGraph, plan, "kmeanssource", "points", dataPointsObject);

    taskExecutor.execute(simpleTaskGraph, plan);
    DataObject<double[][]> dataSet = taskExecutor.getOutput(simpleTaskGraph, plan, "kmeanssink");

//    DataObject<Object> dataSet = taskExecutor.getOutput(simpleTaskGraph, plan, "kmeanssink");
//    DataPartition<Object> values = dataSet.getPartitions()[0];
//    Object lastObject = values.getConsumer().next();
//    LOG.info(String.format("Last Object : %s", lastObject.getClass().getGraphName()));

  }

  public void getParams() {
    workers = config.getIntegerValue(WorkerConstants.WORKERS, 1);
    parallelism = config.getIntegerValue(WorkerConstants.PARALLELISM, 4);
    dataSource = config.getStringValue(MLDataObjectConstants.TRAINING_DATA_DIR, "");
  }

  private static class SimpleDataAllReduceTask extends BaseCompute {

    private static final long serialVersionUID = 5705351508072337994L;

    private Object object;

//    @Override
//    public DataPartition<Object> get() {
//      return new EntityPartition<>(context.taskIndex(), object);
//    }

    @Override
    public boolean execute(IMessage content) {
      object = content.getContent();
      LOG.info(String.format("Object Instance : %s", object.getClass().getName()));
      return true;
    }
  }

  private class DataSourceTask extends BaseSource {
    private static final long serialVersionUID = -1836625523925581215L;

    private Object object = null;

    private DataObject<?> dataPointsObject = null;

//    @Override
//    public void add(String name, DataObject<?> data) {
//      if ("points".equals(name)) {
//        this.dataPointsObject = data;
//      }
//    }

    @Override
    public void execute() {
      // object = getTaskIndexDataPoints(context.taskIndex());
      context.writeEnd("all-reduce", "s");
    }

    private Object getTaskIndexDataPoints(int taskIndex) {
      EntityPartition<Object> datapointsEntityPartition
          = (EntityPartition<Object>) dataPointsObject.getPartition(taskIndex);
      if (datapointsEntityPartition != null) {
        DataObject<?> dataObject
            = (DataObject<?>) datapointsEntityPartition.getConsumer().next();
        object = getDataObjects(taskIndex, dataObject);
      }
      return object;
    }

    public Object getDataObjects(int taskIndex, DataObject<?> datapointsDataObject) {
      Iterator<ArrayList> arrayListIterator = (Iterator<ArrayList>)
          datapointsDataObject.getPartition(taskIndex).getConsumer().next();
      List<Object> allObjects = new ArrayList<>();

      while (arrayListIterator.hasNext()) {
        Object object1 = arrayListIterator.next();
        allObjects.add(object1);
      }
      return allObjects.get(0);
    }

  }

  public class SimpleDataAggregator implements IFunction {

    private static final long serialVersionUID = 4948225063068433511L;

    private Object object;

    @Override
    public Object onMessage(Object object1, Object object2) {
      object = object1;
      LOG.info(String.format("Object Types : %s, %s", object1.getClass().getName(),
          object2.getClass().getName()));
      return object;
    }
  }

}


