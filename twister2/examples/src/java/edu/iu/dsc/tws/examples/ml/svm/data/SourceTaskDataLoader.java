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

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.data.utils.MLDataObjectConstants;
import edu.iu.dsc.tws.data.utils.WorkerConstants;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;


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
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
//    DataObjectSource sourceTask = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
//        dataSource);
//    DataObjectSink sinkTask = new DataObjectSink();
//    taskGraphBuilder.addSource("datapointsource", sourceTask, parallelism);
//    ComputeConnection firstGraphComputeConnection = taskGraphBuilder.addSink(
//        "datapointsink", sinkTask, parallelism);
//    firstGraphComputeConnection.direct("datapointsource",
//        Context.TWISTER2_DIRECT_EDGE, DataType.OBJECT);
//    taskGraphBuilder.setMode(OperationMode.BATCH);
//
//    DataFlowTaskGraph datapointsTaskGraph = taskGraphBuilder.build();
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
    taskGraphBuilder.addSource("kmeanssource", kMeansSourceTask, parallelism);
    ComputeConnection computeConnection = taskGraphBuilder.addSink(
        "kmeanssink", kMeansAllReduceTask, parallelism);
    computeConnection.allreduce("kmeanssource", "all-reduce",
        new SimpleDataAggregator(), DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph simpleTaskGraph = taskGraphBuilder.build();
    ExecutionPlan plan = taskExecutor.plan(simpleTaskGraph);
//    taskExecutor.addInput(
//        simpleTaskGraph, plan, "kmeanssource", "points", dataPointsObject);

    taskExecutor.execute(simpleTaskGraph, plan);
    DataObject<double[][]> dataSet = taskExecutor.getOutput(simpleTaskGraph, plan, "kmeanssink");

//    DataObject<Object> dataSet = taskExecutor.getOutput(simpleTaskGraph, plan, "kmeanssink");
//    DataPartition<Object> values = dataSet.getPartitions()[0];
//    Object lastObject = values.getConsumer().next();
//    LOG.info(String.format("Last Object : %s", lastObject.getClass().getName()));

  }

  public void getParams() {
    workers = config.getIntegerValue(WorkerConstants.WORKERS, 1);
    parallelism = config.getIntegerValue(WorkerConstants.PARALLELISM, 4);
    dataSource = config.getStringValue(MLDataObjectConstants.TRAINING_DATA_DIR, "");
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
          = (EntityPartition<Object>) dataPointsObject.getPartitions(taskIndex);
      if (datapointsEntityPartition != null) {
        DataObject<?> dataObject
            = (DataObject<?>) datapointsEntityPartition.getConsumer().next();
        object = getDataObjects(taskIndex, dataObject);
      }
      return object;
    }

    public Object getDataObjects(int taskIndex, DataObject<?> datapointsDataObject) {
      Iterator<ArrayList> arrayListIterator = (Iterator<ArrayList>)
          datapointsDataObject.getPartitions(taskIndex).getConsumer().next();
      List<Object> allObjects = new ArrayList<>();

      while (arrayListIterator.hasNext()) {
        Object object1 = arrayListIterator.next();
        allObjects.add(object1);
      }
      return allObjects.get(0);
    }

  }

  private static class SimpleDataAllReduceTask extends BaseSink {

    private static final long serialVersionUID = 5705351508072337994L;

    private Object object;

//    @Override
//    public DataPartition<Object> get() {
//      return new EntityPartition<>(context.taskIndex(), object);
//    }

    @Override
    public boolean execute(IMessage content) {
      object = (Object) content.getContent();
      LOG.info(String.format("Object Instance : %s", object.getClass().getName()));
      return true;
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


