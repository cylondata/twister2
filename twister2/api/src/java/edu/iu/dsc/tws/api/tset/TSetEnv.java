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
package edu.iu.dsc.tws.api.tset;

import edu.iu.dsc.tws.api.task.TaskExecutor;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class TSetEnv{

  private Config config;

  private TSetBuilder tSetBuilder;

  private TaskExecutor taskExecutor;

  public TSetEnv(Config config, TaskExecutor taskExecutor) {
    this.config = config;
    this.taskExecutor = taskExecutor;
    this.tSetBuilder = TSetBuilder.newBuilder(config);
  }

  public Config getConfig() {
    return config;
  }

  public <T> TSet<T> createSource(Source<T> source) {
    return this.tSetBuilder.createSource(source);
  }

  public void setMode(OperationMode mode){
    this.tSetBuilder.setMode(mode);
  }

  public void run(){ // todo: is this the best name? or should this be a method in the tset?
    DataFlowTaskGraph graph = tSetBuilder.build();
    ExecutionPlan executionPlan = taskExecutor.plan(graph);
    this.taskExecutor.execute(graph, executionPlan);
  }
}
