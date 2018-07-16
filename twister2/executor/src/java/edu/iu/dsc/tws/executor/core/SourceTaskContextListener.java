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
package edu.iu.dsc.tws.executor.core;

import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.executor.INodeInstance;
import edu.iu.dsc.tws.task.api.TaskContext;

public class SourceTaskContextListener extends TaskContextListener {

  private TaskContext context;
  private INodeInstance iNodeInstance;
  private HashMap<INodeInstance, TaskContext> instanceContextMap = new HashMap<>();

  public SourceTaskContextListener(TaskContext context, INodeInstance iNodeInstance) {
    this.context = context;
    this.iNodeInstance = iNodeInstance;
  }

  @Override
  public void contextStore(HashMap<INodeInstance, TaskContext> instanceContext) {
    super.contextStore(instanceContext);
    instanceContext.put(this.iNodeInstance, this.context);
    this.instanceContextMap = instanceContext;
  }

  public void interruptSourceTask() {
    for(Map.Entry<INodeInstance, TaskContext> e : this.instanceContextMap.entrySet()) {

    }
  }
}
