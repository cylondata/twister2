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
package edu.iu.dsc.tws.examples.testing;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.task.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerParameters;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerUtils;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.TaskGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;

public class TestingWorker extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(TestingWorker.class.getName());

  public static DataFlowTaskGraph buildStreamingTaskGraph(String dataDirectory,
                                                          int parallelismValue,
                                                          Config conf) {
    TestingDataObjectStreamingSource dataObjectSource = new TestingDataObjectStreamingSource(
        Context.TWISTER2_DIRECT_EDGE, dataDirectory);
    TestingDataObjectStreamingSink dataObjectSink = new TestingDataObjectStreamingSink();
    TaskGraphBuilder datapointsTaskGraphBuilder = TaskGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsTaskGraphBuilder.addSource("streamingsource", dataObjectSource,
        parallelismValue);
    ComputeConnection firstGraphComputeConnection = datapointsTaskGraphBuilder.addSink(
        "streamingsink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    firstGraphComputeConnection.direct("streamingsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);

    datapointsTaskGraphBuilder.setMode(OperationMode.STREAMING);
    datapointsTaskGraphBuilder.setTaskGraphName("streamingTG");

    //Build the first taskgraph
    return datapointsTaskGraphBuilder.build();
  }

  public static DataFlowTaskGraph buildBatchTaskGraph(String dataDirectory, int parallelismValue,
                                                      Config conf) {
    TestingDataObjectBatchSource dataObjectSource = new TestingDataObjectBatchSource(
        Context.TWISTER2_DIRECT_EDGE, dataDirectory);
    TestingDataObjectBatchSink dataObjectSink = new TestingDataObjectBatchSink();
    TaskGraphBuilder datapointsTaskGraphBuilder = TaskGraphBuilder.newBuilder(conf);

    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    datapointsTaskGraphBuilder.addSource("batchsource", dataObjectSource,
        parallelismValue);
    ComputeConnection firstGraphComputeConnection = datapointsTaskGraphBuilder.addSink(
        "batchsink", dataObjectSink, parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    firstGraphComputeConnection.direct("batchsource")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);

    datapointsTaskGraphBuilder.setMode(OperationMode.BATCH);
    datapointsTaskGraphBuilder.setTaskGraphName("batchTG");

    //Build the batch taskgraph
    return datapointsTaskGraphBuilder.build();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute() {
    LOG.log(Level.FINE, "Task worker starting: " + workerId);

    KMeansWorkerParameters kMeansJobParameters = KMeansWorkerParameters.build(config);
    KMeansWorkerUtils workerUtils = new KMeansWorkerUtils(config);

    int parallelismValue = kMeansJobParameters.getParallelismValue();
    int dimension = kMeansJobParameters.getDimension();
    int numFiles = kMeansJobParameters.getNumFiles();
    int dsize = kMeansJobParameters.getDsize();
    int csize = kMeansJobParameters.getCsize();
    int iterations = kMeansJobParameters.getIterations();

    String dataDirectory = kMeansJobParameters.getDatapointDirectory() + workerId;
    String centroidDirectory = kMeansJobParameters.getCentroidDirectory() + workerId;

    workerUtils.generateDatapoints(dimension, numFiles, dsize, csize, dataDirectory,
        centroidDirectory);

    long startTime = System.currentTimeMillis();

    /* First Graph to partition and read the partitioned data points **/
    DataFlowTaskGraph datapointsTaskGraph = buildStreamingTaskGraph(dataDirectory,
        parallelismValue, config);
    //Get the execution plan for the first task graph
    ExecutionPlan firstGraphExecutionPlan = taskExecutor.plan(datapointsTaskGraph);
    //Actual execution for the first taskgraph
    //taskExecutor.execute(datapointsTaskGraph, firstGraphExecutionPlan);
    taskExecutor.iExecute(datapointsTaskGraph, firstGraphExecutionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> dataPointsObject = taskExecutor.getOutput(
        datapointsTaskGraph, firstGraphExecutionPlan, "datapointsink");


    /* Second Graph to read the centroids **/
    DataFlowTaskGraph centroidsTaskGraph = buildBatchTaskGraph(centroidDirectory,
        parallelismValue, config);
    //Get the execution plan for the second task graph
    ExecutionPlan secondGraphExecutionPlan = taskExecutor.plan(centroidsTaskGraph);
    //Actual execution for the second taskgraph
    taskExecutor.execute(centroidsTaskGraph, secondGraphExecutionPlan);
    //Retrieve the output of the first task graph
    DataObject<Object> centroidsDataObject = taskExecutor.getOutput(
        centroidsTaskGraph, secondGraphExecutionPlan, "centroidsink");
  }
}

