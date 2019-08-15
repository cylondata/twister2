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
package edu.iu.dsc.tws.examples.task.dataparallel;

import java.io.IOException;

import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.examples.comms.Constants;
import edu.iu.dsc.tws.examples.utils.DataGenerator;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskWorker;

/**
 * This example demonstrates the use of a data parallel job. Here we will generate set of random
 * data using the data api and process it in parallel by loading the saved data using the data
 * API
 */
public class DataParallelWorker extends TaskWorker {

  @Override
  public void execute() {
    ComputeGraphBuilder computeGraphBuilder = ComputeGraphBuilder.newBuilder(config);

    String inputDirectory = config.getStringValue(Constants.ARGS_INPUT_DIRECTORY);
    boolean shared = config.getBooleanValue(Constants.ARGS_SHARED_FILE_SYSTEM);
    int numFiles = config.getIntegerValue(Constants.ARGS_NUMBER_OF_FILES, 4);
    int size = config.getIntegerValue(Constants.ARGS_SIZE, 1000);
    int parallel = config.getIntegerValue(Constants.ARGS_PARALLEL, 2);

    if (!shared && workerId == 0) {
      try {
        DataGenerator.generateData("txt", new Path(inputDirectory), numFiles, size, 10);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create data: " + inputDirectory);
      }
    }

    DataParallelTask task = new DataParallelTask();
    computeGraphBuilder.addSource("map", task, parallel);
    computeGraphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph dataFlowTaskGraph = computeGraphBuilder.build();
    ExecutionPlan plan = taskExecutor.plan(dataFlowTaskGraph);
    taskExecutor.execute(dataFlowTaskGraph, plan);
  }

}
