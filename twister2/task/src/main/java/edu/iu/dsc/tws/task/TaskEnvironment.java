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

package edu.iu.dsc.tws.task;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.Network;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.task.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.task.impl.TaskExecutor;
import edu.iu.dsc.tws.task.impl.TaskGraphBuilder;

public final class TaskEnvironment {

  private static final Logger LOG = Logger.getLogger(TaskEnvironment.class.getName());
  private Communicator communicator;
  private TaskExecutor taskExecutor;
  private Config config;
  private IWorkerController wController;
  private static int taskGraphIndex = 0;

  private TaskEnvironment(Config config, int workerId,
                          IWorkerController wController, IVolatileVolume vVolume) {
    this.config = config;
    this.wController = wController;
    List<JobMasterAPI.WorkerInfo> workerInfoList = null;
    try {
      workerInfoList = wController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }

    // create the channel
    TWSChannel channel = Network.initializeChannel(config, wController);
    String persistent = null;
    if (vVolume != null && vVolume.getWorkerDirPath() != null) {
      persistent = vVolume.getWorkerDirPath();
    }
    // create the communicator
    this.communicator = new Communicator(config, channel, persistent);
    // create the executor
    this.taskExecutor = new TaskExecutor(config, workerId, workerInfoList,
        communicator, wController.getCheckpointingClient());

  }

  private TaskEnvironment(WorkerEnvironment workerEnv) {
    this.config = workerEnv.getConfig();
    this.wController = workerEnv.getWorkerController();
    this.communicator = workerEnv.getCommunicator();
    this.taskExecutor = new TaskExecutor(workerEnv);
  }


  /**
   * Use task executor for fine grained task graph manipulations. For single task graph builds,
   * use @buildAndExecute
   *
   * @return taskExecutor
   */
  public TaskExecutor getTaskExecutor() {
    return taskExecutor;
  }

  /**
   * for single task graph runs
   */
  public TaskExecutor buildAndExecute(TaskGraphBuilder taskGraphBuilder) {
    DataFlowTaskGraph dataFlowTaskGraph = taskGraphBuilder.build();
    ExecutionPlan plan = this.getTaskExecutor().plan(dataFlowTaskGraph);
    this.getTaskExecutor().execute(dataFlowTaskGraph, plan);
    return this.getTaskExecutor();
  }

  public  Map<String, ExecutionPlan> build(DataFlowTaskGraph ...dataFlowTaskGraphs) {
    return this.getTaskExecutor().plan(dataFlowTaskGraphs);
  }

  public TaskGraphBuilder newTaskGraph(OperationMode operationMode) {
    return this.newTaskGraph(operationMode, "task-graph-" + (taskGraphIndex++));
  }

  public TaskGraphBuilder newTaskGraph(OperationMode operationMode, String name) {
    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(this.config);
    taskGraphBuilder.setMode(operationMode);
    taskGraphBuilder.setTaskGraphName(name);
    return taskGraphBuilder;
  }

  public static TaskEnvironment init(Config config, int workerId,
                                     IWorkerController wController, IVolatileVolume vVolume) {
    return new TaskEnvironment(config, workerId, wController, vVolume);
  }

  public static TaskEnvironment init(WorkerEnvironment workerEnv) {
    return new TaskEnvironment(workerEnv);
  }

  public void close() {
    try {
      wController.waitOnBarrier();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
    }
    // close the task executor
    taskExecutor.close();
    // lets terminate the network
    communicator.close();
  }
}
