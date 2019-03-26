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

import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.api.task.TaskExecutor;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.api.tset.sets.StreamingSourceTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class TSetEnv {

  private Config config;

  private TSetBuilder tSetBuilder;

  private TaskExecutor taskExecutor;

  private Map<String, Map<String, Cacheable<?>>> inputMap;

  public TSetEnv(Config config, TaskExecutor taskExecutor) {
    this.config = config;
    this.taskExecutor = taskExecutor;
    this.tSetBuilder = TSetBuilder.newBuilder(config);
    inputMap = new HashMap<>();
  }

  public TSetEnv(Config config, TaskExecutor taskExecutor, OperationMode mode) {
    this.config = config;
    this.taskExecutor = taskExecutor;
    this.tSetBuilder = TSetBuilder.newBuilder(config);
    this.tSetBuilder.setMode(mode);
    inputMap = new HashMap<>();
  }

  public Config getConfig() {
    return config;
  }

  public <T> BatchSourceTSet<T> createBatchSource(Source<T> source, int parallelism) {
    return this.tSetBuilder.createBatchSource(source, parallelism, this);
  }

  public <T> StreamingSourceTSet<T> createStreamingSource(Source<T> source, int parallelism) {
    return this.tSetBuilder.createStreamingSource(source, parallelism, this);
  }

  public void setMode(OperationMode mode) {
    this.tSetBuilder.setMode(mode);
  }

  public TSetBuilder getTSetBuilder() {
    return tSetBuilder;
  }

  public void settSetBuilder(TSetBuilder tSBuilder) {
    this.tSetBuilder = tSBuilder;
  }

  public void run() { // todo: is this the best name? or should this be a method in the tset?
    DataFlowTaskGraph graph = tSetBuilder.build();
    ExecutionPlan executionPlan = taskExecutor.plan(graph);
    pushInputsToFunctions(graph, executionPlan);
    this.taskExecutor.execute(graph, executionPlan);
  }

  public <T> DataObject<T> runAndGet(String sinkName) {
    DataFlowTaskGraph graph = tSetBuilder.build();
    ExecutionPlan executionPlan = taskExecutor.plan(graph);
    pushInputsToFunctions(graph, executionPlan);
    this.taskExecutor.execute(graph, executionPlan);
    return this.taskExecutor.getOutput(graph, executionPlan, sinkName);
  }

  public void addInput(String taskName, String key, Cacheable<?> input) {
    Map temp = inputMap.getOrDefault(taskName, new HashMap<>());
    temp.put(key, input);
    inputMap.put(taskName, temp);
  }

  /**
   * pushes the inputs into each task before the task execution is done
   *
   * @param executionPlan the built execution plan
   */
  private void pushInputsToFunctions(DataFlowTaskGraph graph, ExecutionPlan executionPlan) {
    for (String taskName : inputMap.keySet()) {
      Map<String, Cacheable<?>> tempMap = inputMap.get(taskName);
      for (String keyName : tempMap.keySet()) {
        taskExecutor.addInput(graph, executionPlan, taskName,
            keyName, tempMap.get(keyName).getDataObject());
      }
    }
  }
}
