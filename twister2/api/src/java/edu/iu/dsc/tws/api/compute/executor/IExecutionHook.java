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
package edu.iu.dsc.tws.api.compute.executor;

public interface IExecutionHook {
  /**
   * Every IExecutor should execute this method before it begins an execution
   */
  void beforeExecution();

  /**
   * Every IExecitor should execute this method after it finishes an execution
   */
  void afterExecution();
}
