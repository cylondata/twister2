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
package edu.iu.dsc.tws.executor.threading;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.IExecutor;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class Executor {
  private ExecutionPlan executionPlan;

  private TWSChannel channel;

  private OperationMode operationMode;

  private Config config;

  private int workerId;

  public Executor(Config cfg, int wId, ExecutionPlan executionPlan,
                  TWSChannel channel) {
    this(cfg, wId, executionPlan, channel, OperationMode.STREAMING);
  }

  public Executor(Config cfg, int wId, ExecutionPlan executionPlan,
                  TWSChannel channel, OperationMode operationMode) {
    this.executionPlan = executionPlan;
    this.channel = channel;
    this.operationMode = operationMode;
    this.config = cfg;
    this.workerId = wId;
  }

  /***
   * Communication Channel must be progressed after the task execution model
   * is initialized. It must be progressed only after execution is instantiated.
   * */
  public boolean execute() {
    // lets start the execution
    IExecutor executor;
    if (operationMode == OperationMode.STREAMING) {
      executor = new StreamingShareingExecutor(workerId);
    } else {
      executor = new BatchSharingExecutor(workerId);
    }

    return executor.execute(config, executionPlan, channel);
  }
}
