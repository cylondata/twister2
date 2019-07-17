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
package edu.iu.dsc.tws.api.tset;

import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.task.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.fn.Source;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.api.tset.sets.streaming.StreamingSourceTSet;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

public final class TSetEnvironment {

  private WorkerEnvironment workerEnv;
  private TSetGraph graph;
  private TaskExecutor taskExecutor;
  private OperationMode operationMode;

  private int defaultParallelism = 1;
  private Map<String, Map<String, Cacheable<?>>> inputMap = new HashMap<>();

  private static volatile TSetEnvironment thisTSetEnv;

  private TSetEnvironment(WorkerEnvironment wEnv, OperationMode opMode) {
    this.workerEnv = wEnv;
    this.operationMode = opMode;

    this.graph = new TSetGraph(this, opMode);
    this.taskExecutor = new TaskExecutor(workerEnv);
  }

  public void setDefaultParallelism(int defaultParallelism) {
    this.defaultParallelism = defaultParallelism;
  }

  public OperationMode getOperationMode() {
    return this.operationMode;
  }

  public TSetGraph getGraph() {
    return graph;
  }

  public int getDefaultParallelism() {
    return defaultParallelism;
  }

  public Config getConfig() {
    return workerEnv.getConfig();
  }

  // todo: find a better OOP way of doing this!
  public <T> BatchSourceTSet<T> createBatchSource(Source<T> source, int parallelism) {
    if (operationMode == OperationMode.STREAMING) {
      throw new RuntimeException("Batch sources can not be created in streaming mode");
    }

    BatchSourceTSet<T> sourceT = new BatchSourceTSet<>(this, source, parallelism);
    graph.addTSet(sourceT);

    return sourceT;
  }

  public <T> StreamingSourceTSet<T> createStreamingSource(Source<T> source, int parallelism) {
    if (operationMode == OperationMode.BATCH) {
      throw new RuntimeException("Streaming sources can not be created in batch mode");
    }

    StreamingSourceTSet<T> sourceT = new StreamingSourceTSet<>(this, source, parallelism);
    graph.addTSet(sourceT);

    return sourceT;
  }

  public void run() { // todo: is this the best name? or should this be a method in the tset?
//    DataFlowTaskGraph graph = tSetBuilder.build();
//    ExecutionPlan executionPlan = taskExecutor.plan(graph);
//    pushInputsToFunctions(graph, executionPlan);
//    this.taskExecutor.execute(graph, executionPlan);
  }

  public <T> DataObject<T> runAndGet(String sinkName) {
//    DataFlowTaskGraph graph = tSetBuilder.build();
//    ExecutionPlan executionPlan = taskExecutor.plan(graph);
//    pushInputsToFunctions(graph, executionPlan);
//    this.taskExecutor.execute(graph, executionPlan);
//    return this.taskExecutor.getOutput(graph, executionPlan, sinkName);
    return null;
  }

  public void addInput(String taskName, String key, Cacheable<?> input) {
    Map<String, Cacheable<?>> temp = inputMap.getOrDefault(taskName, new HashMap<>());
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

//  /**
//   * reset the Env so that it can be reused for the next action
//   * This method will reset the {@link TSetEnvironment#tSetBuilder} and clears all the values
//   in the
//   * {@link TSetEnvironment#inputMap}
//   */
/*  public void reset() {
    settSetBuilder(TSetBuilder.newBuilder(config).setMode(tSetBuilder.getOpMode()));
    inputMap.clear();
  }*/

  /**
   * initialize the Tset environment
   *
   * @param wEnv worker environment
   * @param opMode operation mode
   * @return tset environment
   */
  public static TSetEnvironment init(WorkerEnvironment wEnv, OperationMode opMode) {
    if (thisTSetEnv == null) {
      synchronized (TSetEnvironment.class) {
        if (thisTSetEnv == null) {
          thisTSetEnv = new TSetEnvironment(wEnv, opMode);
        }
      }
    }

    return thisTSetEnv;
  }

  /**
   * initialize the Tset environment
   *
   * @param wEnv worker environment
   * @return tset environment for batch operation
   */
  public static TSetEnvironment init(WorkerEnvironment wEnv) {
    return init(wEnv, OperationMode.BATCH);
  }
}
