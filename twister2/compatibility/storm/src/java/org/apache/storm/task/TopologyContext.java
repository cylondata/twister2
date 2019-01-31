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

package org.apache.storm.task;

import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * A TopologyContext is given to bolts and spouts in their "prepare" and "open"
 * methods, respectively. This object provides information about the component's
 * place within the topology, such as task ids, inputs and outputs, etc.
 * <p>The TopologyContext is also used to declare ISubscribedState objects to
 * synchronize state with StateSpouts this object is subscribed to.
 */
public class TopologyContext {

  private TaskContext t2TaskContext;

  public TopologyContext(TaskContext t2TaskContext) {
    this.t2TaskContext = t2TaskContext;
  }

  public int getThisTaskId() {
    return this.t2TaskContext.taskId();
  }

  public String getThisComponentId() {
    return this.t2TaskContext.taskName();
  }

  //todo implement other methods if required
}
