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

import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.comm.tasks.batch.SourceBatchTask;
import edu.iu.dsc.tws.task.api.SourceTask;
import edu.iu.dsc.tws.task.api.TaskContext;

public class SourceTaskContextListener extends TaskContextListener {

  private TaskContext context;
  private SourceTask sourceTask;
  private SourceBatchTask sourceBatchTask;
  private HashMap<SourceBatchTask, TaskContext> instanceBatchContextMap = new HashMap<>();

  public SourceTaskContextListener() {
  }

  public SourceTaskContextListener(SourceTask sourceTask, TaskContext context) {
    this.context = context;
    this.sourceTask = sourceTask;
  }

  public SourceTaskContextListener(SourceBatchTask sourceBatchTask, TaskContext context) {
    this.context = context;
    this.sourceBatchTask = sourceBatchTask;
  }

  @Override
  public void contextStore() {
    //this.instanceContextMap.put(this.sourceTask, this.context);
  }

  @Override
  public void onStop() {

  }

  @Override
  public void onStart() {
    this.instanceBatchContextMap.put(this.sourceBatchTask, this.context);
    this.addTaskContext((INodeInstance) this.sourceBatchTask, this.context);
  }

  @Override
  public void onInterrupt() {
    for (Map.Entry<SourceBatchTask, TaskContext> e : this.instanceBatchContextMap.entrySet()) {
      if (e.getValue().isDone()) {
        SourceBatchTask currentSourceTask = e.getKey();
        TaskContext newContext = e.getValue();
        newContext.setDone(newContext.isDone());
        this.instanceBatchContextMap.put(currentSourceTask, newContext);
      }
    }
  }

  @Override
  public void mutateContext(TaskContext ctx) {
    this.context = ctx;
  }

  public HashMap<SourceBatchTask, TaskContext> getInstanceBatchContextMap() {
    return this.instanceBatchContextMap;
  }

  public void setInstanceBatchContextMap(HashMap<SourceBatchTask, TaskContext>
                                             instanceBatchContextMap) {
    this.instanceBatchContextMap = instanceBatchContextMap;
  }
}
