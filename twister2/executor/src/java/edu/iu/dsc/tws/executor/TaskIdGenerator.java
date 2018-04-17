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
package edu.iu.dsc.tws.executor;

/**
 * This is a global task id generator depending on the taskId, task index and task name
 */
public class TaskIdGenerator {

  /**
   * Generate a unique global task id
   * @param taskName name of the task
   * @param taskId task id
   * @param taskIndex task index
   * @return the global task id
   */
  public int generateGlobalTaskId(String taskName, int taskId, int taskIndex) {
    return taskId * 100000 + taskIndex;
  }
}
