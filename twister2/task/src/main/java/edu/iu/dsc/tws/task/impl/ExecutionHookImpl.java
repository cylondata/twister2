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
package edu.iu.dsc.tws.task.impl;

import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.executor.IExecutionHook;
import edu.iu.dsc.tws.api.compute.executor.IExecutor;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;

public class ExecutionHookImpl implements IExecutionHook {
  /**
   * Keep the data objects with name -> object
   */
  private Map<String, DataObject> dataObjectMap = new HashMap<>();

  /**
   * The execution plan
   */
  private ExecutionPlan plan;

  /**
   * The config
   */
  private Config config;

  /**
   * The current executors list
   */
  private ExecutorList executors;

  public ExecutionHookImpl(Config cfg, Map<String, DataObject> dataObjectMap, ExecutionPlan plan,
                           ExecutorList exList) {
    this.dataObjectMap = dataObjectMap;
    this.plan = plan;
    this.config = cfg;
    this.executors = exList;
  }

  @Override
  public void beforeExecution() {
    TaskExecutor.distributeData(plan, dataObjectMap);
  }

  @Override
  public void afterExecution() {
    TaskExecutor.collectData(config, plan, dataObjectMap);
  }

  @Override
  public void onClose(IExecutor ex) {
    executors.remove(ex);
  }
}
