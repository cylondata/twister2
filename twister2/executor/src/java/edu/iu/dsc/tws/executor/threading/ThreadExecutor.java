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

import java.util.concurrent.locks.ReentrantLock;

import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.executor.api.ExecutionModel;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.IThreadExecutor;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class ThreadExecutor implements IThreadExecutor {

  private ExecutionModel executionModel;

  private ExecutionPlan executionPlan;

  private TWSChannel channel;

  private OperationMode operationMode;

  private boolean progress = true;

  private boolean isExecutionFinished = false;

  private ReentrantLock lock = new ReentrantLock();

  public ThreadExecutor() {

  }

  public ThreadExecutor(ExecutionModel executionModel, ExecutionPlan executionPlan) {
    this.executionModel = executionModel;
    this.executionPlan = executionPlan;
  }

  public ThreadExecutor(ExecutionModel executionModel, ExecutionPlan executionPlan,
                        TWSChannel channel) {
    this.executionModel = executionModel;
    this.executionPlan = executionPlan;
    this.channel = channel;
  }

  public ThreadExecutor(ExecutionModel executionModel, ExecutionPlan executionPlan,
                        TWSChannel channel, OperationMode operationMode) {
    this.executionModel = executionModel;
    this.executionPlan = executionPlan;
    this.channel = channel;
    this.operationMode = operationMode;
  }

  /***
   * Communication Channel must be progressed after the task execution model
   * is initialized. It must be progressed only after execution is instantiated.
   * */
  @Override
  public boolean execute() {
    // lets start the execution
    ThreadExecutorFactory threadExecutorFactory = new ThreadExecutorFactory(executionModel,
        this, executionPlan, channel, operationMode);
    isExecutionFinished = threadExecutorFactory.execute();
    System.out.println("All Execution Finished : " + isExecutionFinished);
    //progressComms();
    return isExecutionFinished;
  }

  /**
   * Progresses the communication Channel
   */
  public void progressComms() {
    while (true) {
      this.channel.progress();
    }
  }

}
