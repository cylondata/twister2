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

import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.dataobjects.DataObjectSink;
import edu.iu.dsc.tws.api.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.data.utils.MLDataObjectConstants;
import edu.iu.dsc.tws.data.utils.WorkerConstants;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class TaskWorkerDataLoader extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(TaskWorkerDataLoader.class.getName());

  private static int workers = 1;
  private static int parallelism = 4;
  private static String dataSource = "";

  @Override
  public void execute() {
    getParams();
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    DataObjectSource sourceTask = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataSource);
    DataObjectSink sinkTask = new DataObjectSink();
    taskGraphBuilder.addSource("datapointsource", sourceTask, parallelism);
    ComputeConnection firstGraphComputeConnection = taskGraphBuilder.addSink(
        "datapointsink", sinkTask, parallelism);
    firstGraphComputeConnection.direct("datapointsource",
        Context.TWISTER2_DIRECT_EDGE, DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph datapointsTaskGraph = taskGraphBuilder.build();
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, "datapointsink");
    LOG.info("Total Partitions : " + dataPointsObject.getPartitions().length);
    showAllUnits(dataPointsObject);
  }

  public void getParams() {
    workers = config.getIntegerValue(WorkerConstants.WORKERS, 1);
    parallelism = config.getIntegerValue(WorkerConstants.PARALLELISM, 4);
    dataSource = config.getStringValue(MLDataObjectConstants.TRAINING_DATA_DIR, "");
  }


  public void showAllUnits(DataObject<Object> dataPointsObject) {
    for (int i = 0; i < dataPointsObject.getPartitions().length; i++) {
      DataPartition<Object> values = dataPointsObject.getPartitions()[i];
      DataPartitionConsumer<Object> dataPartitionConsumer = values.getConsumer();
      //LOG.info("Final Receive  : " + dataPartitionConsumer.hasNext());
      while (dataPartitionConsumer.hasNext()) {
        LOG.info(String.format("Id1[%d], Type: %s", i,
            dataPartitionConsumer.next().getClass().getName()));
        Object object = dataPartitionConsumer.next();
        if (object instanceof DataObjectImpl<?>) {
          DataObjectImpl<?> dataObjectImpl = (DataObjectImpl<?>) object;
          LOG.info(String.format("Id1[%d], Partition Count :  %d", i,
              dataObjectImpl.getPartitionCount()));
          int numpar = dataObjectImpl.getPartitions().length;
          LOG.info("Number of Partitions : " + numpar);
          for (int j = 0; j < dataObjectImpl.getPartitions().length; j++) {
            DataPartition<?> values1 = dataObjectImpl.getPartitions()[j];
            Object object1 = values1.getConsumer().next();
            LOG.info(String.format("Ids[%d,%d] , Received Object : %s ", i, j,
                object1.getClass().getName()));
            if (object1 instanceof Iterator<?>) {
              Iterator<?> itr = (Iterator<?>) object1;
              while (itr.hasNext()) {
                Object object2 = itr.next();
                if (object2 instanceof String) {
                  LOG.info(String.format("Ids[%d,%d] , Worker Id %d / %d, Data : %s", i, j,
                      workerId, workers, String.valueOf(object2)));
                }

              }
            }
          }


        }
      }

    }

  }


}
