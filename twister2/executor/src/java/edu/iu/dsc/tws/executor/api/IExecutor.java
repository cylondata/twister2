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
package edu.iu.dsc.tws.executor.api;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TWSChannel;

public interface IExecutor {
  /**
   * Execute the specific plan
   * @param cfg configuration
   * @param plan execution plan
   * @param channel the communication channel
   * @return true if accepted
   */
  boolean execute(Config cfg, ExecutionPlan plan, TWSChannel channel);

  /**
   * Asynchronously execute a plan, One need to call progress on the execution object returned to
   * continue the execution
   * @param cfg configuration
   * @param plan execution plan
   * @param channel the communication channel
   * @return an execution or null if not accepted
   */
  IExecution iExecute(Config cfg, ExecutionPlan plan, TWSChannel channel);
}
