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
package edu.iu.dsc.tws.examples.internal.taskgraph;


import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.dataobjects.DataObjectSource;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataObjectCompute;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansDataObjectDirectSink;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerParameters;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerUtils;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

/**
 * This is the first part of the k-means algorithm which has been ported here to test
 * and accomodate the local file system for the data locality task scheduling.
 */
public class DataLocalityBatchTaskGraphExample extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(
      DataLocalityBatchTaskGraphExample.class.getName());

  @SuppressWarnings("unchecked")
  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    KMeansWorkerParameters kMeansJobParameters = KMeansWorkerParameters.build(config);
    KMeansWorkerUtils workerUtils = new KMeansWorkerUtils(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();
    int csize = kMeansJobParameters.getCsize();

    String dataDirectory = kMeansJobParameters.getDatapointDirectory() + workerId;
    String centroidDirectory = kMeansJobParameters.getCentroidDirectory() + workerId;

    workerUtils.generateDatapoints(dimension, numFiles, dsize, csize, dataDirectory,
        centroidDirectory);

    /* First Graph to partition and read the partitioned data points **/
    DataObjectSource dataObjectSource = new DataObjectSource(Context.TWISTER2_DIRECT_EDGE,
        dataDirectory);
    KMeansDataObjectCompute dataObjectCompute = new KMeansDataObjectCompute(
        Context.TWISTER2_DIRECT_EDGE, dsize, parallelismValue, dimension);
    KMeansDataObjectDirectSink dataObjectSink = new KMeansDataObjectDirectSink();
    TaskGraphBuilder datapointsTaskGraphBuilder = TaskGraphBuilder.newBuilder(config);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsTaskGraphBuilder.addSource("datapointsource", dataObjectSource, parallelismValue);
    ComputeConnection datapointComputeConnection = datapointsTaskGraphBuilder.addCompute(
        "datapointcompute", dataObjectCompute, parallelismValue);
    ComputeConnection firstGraphComputeConnection = datapointsTaskGraphBuilder.addSink(
        "datapointsink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    datapointComputeConnection.direct("datapointsource", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    firstGraphComputeConnection.direct("datapointcompute", Context.TWISTER2_DIRECT_EDGE,
        DataType.OBJECT);
    datapointsTaskGraphBuilder.setMode(OperationMode.BATCH);

    //Build the first taskgraph
    DataFlowTaskGraph datapointsTaskGraph = datapointsTaskGraphBuilder.build();
    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, "datapointsink");
    for (int i = 0; i < dataPointsObject.getPartitions().length; i++) {
      LOG.info("Datapoints values:" + Arrays.deepToString(dataPointsObject.getPartitions()));
    }
  }
}
