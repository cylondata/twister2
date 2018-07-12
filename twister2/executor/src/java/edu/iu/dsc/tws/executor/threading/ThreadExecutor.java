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

import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.executor.ExecutionPlan;

public class ThreadExecutor implements IThreadExecutor {

  private ExecutionModel executionModel;

  private ExecutionPlan executionPlan;

  private TWSChannel channel;

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

  /***
   * Communication Channel must be progressed after the task execution model
   * is initialized. It must be progressed only after execution is instantiated.
   * */
  @Override
  public void execute() {
    // lets start the execution
    ThreadExecutorFactory threadExecutorFactory = new ThreadExecutorFactory(executionModel,
        this, executionPlan);
    threadExecutorFactory.execute();
    progressComms();
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
