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

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.examples.comms.Constants;
import edu.iu.dsc.tws.examples.utils.DataGenerator;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

/**
 * This example demonstrates the use of a data parallel job. Here we will generate set of random
 * data using the data api and process it in parallel by loading the saved data using the data
 * API
 */
public class DataParallelWorker extends TaskWorker {

  @Override
  public void execute() {
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(config);

    String inputDirectory = config.getStringValue(Constants.ARGS_INPUT_DIRECTORY);
    boolean shared = config.getBooleanValue(Constants.ARGS_SHARED_FILE_SYSTEM);
    int numFiles = config.getIntegerValue(Constants.ARGS_NUMBER_OF_FILES, 4);
    int size = config.getIntegerValue(Constants.ARGS_SIZE, 1000);

    if (!shared && workerId == 0) {
      try {
        DataGenerator.generateData("txt", new Path(inputDirectory), numFiles, size, 10);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create data: " + inputDirectory);
      }
    }

    DataParallelTask task = new DataParallelTask();
    taskGraphBuilder.addSource("map", task, 4);
    taskGraphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph dataFlowTaskGraph = taskGraphBuilder.build();
    ExecutionPlan plan = taskExecutor.plan(dataFlowTaskGraph);
    taskExecutor.execute(dataFlowTaskGraph, plan);
  }

}
