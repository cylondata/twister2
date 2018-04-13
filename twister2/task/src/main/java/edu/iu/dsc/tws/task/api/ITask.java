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
package edu.iu.dsc.tws.task.api;

import edu.iu.dsc.tws.common.config.Config;

/**
 * Base interface for tasks
 */
public interface ITask extends INode {
  /**
   * Prepare the task to be executed
   * @param cfg the configuration
   * @param collection the output collection
   */
  void prepare(Config cfg, OutputCollection collection);

  /**
   * Code that needs to be executed in the Task
   */
  Message execute();

  /**
   * Code that is executed for a single message
   */
  Message execute(Message content);

  /**
   * Execute with an incoming message
   * @param content
   */
  void run(Message content);

  /**
   * Execute without an incoming message
   */
  void run();

  /**
   * Assign the task name
   */

  String taskName();
}
