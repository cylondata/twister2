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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerParameters;
import edu.iu.dsc.tws.examples.batch.kmeans.KMeansWorkerUtils;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class DataLocalityStreamingTaskGraphExample extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(
      DataLocalityStreamingTaskGraphExample.class.getName());

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

    GeneratorTask dataObjectSource = new GeneratorTask();
    ReceivingTask dataObjectSink = new ReceivingTask();

    //Adding the user-defined constraints to the graph
    Map<String, String> sourceTaskConstraintsMap = new HashMap<>();
    sourceTaskConstraintsMap.put(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "2");
    //sourceTaskConstraintsMap.put(Context.TWISTER2_TASK_INSTANCE_ODD_PARALLELISM, "1");

    Map<String, String> sinkTaskConstraintsMap = new HashMap<>();
    sinkTaskConstraintsMap.put(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "2");
    //sinkTaskConstraintsMap.put(Context.TWISTER2_TASK_INSTANCE_ODD_PARALLELISM, "1");

    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    //Add source, compute, and sink tasks to the task graph builder for the first task graph
    taskGraphBuilder.addSource("datapointsource", dataObjectSource, parallelismValue);
    ComputeConnection computeConnection = taskGraphBuilder.addSink("datapointsink", dataObjectSink,
        parallelismValue);

    //Creating the communication edges between the tasks for the second task graph
    /*datapointComputeConnection.direct("datapointsource", Context.TWISTER2_DIRECT_EDGE,
    DataType.OBJECT);*/
    computeConnection.partition("datapointsource", "partition-edge", DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.STREAMING);

    //Adding graph and node level constraints
    taskGraphBuilder.addNodeConstraints("datapointsource", sourceTaskConstraintsMap);
    taskGraphBuilder.addNodeConstraints("datapointsink", sinkTaskConstraintsMap);
    taskGraphBuilder.addGraphConstraints(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "2");

    //Build the first taskgraph
    DataFlowTaskGraph taskGraph = taskGraphBuilder.build();
    LOG.info("%%% Graph Constraints:%%%" + taskGraph.getGraphConstraints()
        + "\tNode Constraints:%%%" + taskGraph.getNodeConstraints());

    //Get the execution plan for the first task graph
    ExecutionPlan executionPlan = taskExecutor.plan(taskGraph);
    //Actual execution for the first taskgraph
    taskExecutor.execute(taskGraph, executionPlan);
  }

  private static class GeneratorTask extends BaseSource {

    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      this.context = ctx;
      this.config = cfg;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
      boolean wrote = context.write("partition-edge", "Hello");
      if (wrote) {
        count++;
        if (count % 100 == 0) {
          LOG.info(String.format("%d %d Message sent count : %d", context.getWorkerId(),
              context.globalTaskId(), count));
        }
      }
    }
  }

  private static class ReceivingTask extends BaseSink {

    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      this.context = ctx;
      this.config = cfg;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof String) {
        count += ((String) message.getContent()).length();
      }
      LOG.info(String.format("%d %d Message Received count: %d", context.getWorkerId(),
          context.globalTaskId(), count));
      return true;
    }
  }
}
