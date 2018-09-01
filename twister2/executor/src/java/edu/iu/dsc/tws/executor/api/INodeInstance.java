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
package edu.iu.dsc.tws.executor.api;

import edu.iu.dsc.tws.task.api.INode;

public interface INodeInstance {
  /**
   * Get task id
   * @return task id
   */
  default int getId() {
    return 0;
  }

  /**
   * Execute
   * @return true if further execution is needed
   */
  boolean execute();

  /**
   * Prepare for an execution
   */
  void prepare();

  /**
   * Lets reset the node instance for a new execution
   */
  default void reset() {
  }

  /**
   * Get the node of this instance
   * @return the graph node
   */
  INode getNode();
}
