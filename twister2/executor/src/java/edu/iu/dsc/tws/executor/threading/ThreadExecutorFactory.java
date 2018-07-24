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

import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.executor.ExecutionPlan;

public class ThreadExecutorFactory {

  private ExecutionModel executionModel;

  private ThreadExecutor executor;

  private ExecutionPlan executionPlan;

  private TWSNetwork network;

  public ThreadExecutorFactory(ExecutionModel executionModels, ThreadExecutor executor,
                               ExecutionPlan executionPlan) {
    this.executionModel = executionModels;
    this.executor = executor;
    this.executionPlan = executionPlan;
  }

  public ThreadExecutorFactory(ExecutionModel executionModel, ThreadExecutor executor,
                               ExecutionPlan executionPlan, TWSNetwork network) {
    this.executionModel = executionModel;
    this.executor = executor;
    this.executionPlan = executionPlan;
    this.network = network;
  }

  public IThreadExecutor execute() {
    if (ExecutionModel.SHARED.equals(executionModel.getExecutionModel())) {
      ThreadSharingExecutor threadSharingExecutor = new ThreadSharingExecutor(executionPlan);
      threadSharingExecutor.execute();
      return threadSharingExecutor;
    } else if (ExecutionModel.DEDICATED.equals(executionModel.getExecutionModel())) {
      ThreadStaticExecutor threadStaticExecutor = new ThreadStaticExecutor(executionPlan);
      threadStaticExecutor.execute();
      return threadStaticExecutor;
    }

    return null;
  }
}
