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

import edu.iu.dsc.tws.task.api.SinkTask;
import edu.iu.dsc.tws.task.api.TaskContext;

public class SinkTaskContextListener extends TaskContextListener {

  private TaskContext context;
  private SinkTask sinkTask;
  private HashMap<SinkTask, TaskContext> instanceContextMap = new HashMap<>();


  public SinkTaskContextListener() {
  }

  public SinkTaskContextListener(SinkTask sinkTask, TaskContext context) {
    this.context = context;
    this.sinkTask = sinkTask;
  }

  @Override
  public void contextStore() {
    this.instanceContextMap.put(this.sinkTask, this.context);
  }

  @Override
  public void onStop() {

  }

  @Override
  public void onStart() {

  }

  @Override
  public void onInterrupt() {

  }

  @Override
  public void mutateContext(TaskContext ctx) {
    this.context = ctx;
  }

}
