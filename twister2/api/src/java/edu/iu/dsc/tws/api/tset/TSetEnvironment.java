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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.task.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.api.tset.sets.BuildableTSet;
import edu.iu.dsc.tws.api.tset.sets.streaming.StreamingSourceTSet;
import edu.iu.dsc.tws.task.impl.TaskExecutor;

public final class TSetEnvironment {
  private static final Logger LOG = Logger.getLogger(TSetEnvironment.class.getName());

  private WorkerEnvironment workerEnv;
  private TSetGraph tsetGraph;
  private TaskExecutor taskExecutor;
  private OperationMode operationMode;

  private int defaultParallelism = 1;

  private Map<String, Map<String, Cacheable<?>>> tSetInputMap = new HashMap<>();

  private static volatile TSetEnvironment thisTSetEnv;

  private TSetEnvironment(WorkerEnvironment wEnv, OperationMode opMode) {
    this.workerEnv = wEnv;
    this.operationMode = opMode;

    this.tsetGraph = new TSetGraph(this, opMode);

    // can not use task env at the moment because it does not support graph builder API
    this.taskExecutor = new TaskExecutor(workerEnv);
  }

  public void setDefaultParallelism(int defaultParallelism) {
    this.defaultParallelism = defaultParallelism;
  }

  public OperationMode getOperationMode() {
    return this.operationMode;
  }

  public TSetGraph getGraph() {
    return tsetGraph;
  }

  public int getDefaultParallelism() {
    return defaultParallelism;
  }

  public Config getConfig() {
    return workerEnv.getConfig();
  }

  public int getWorkerID() {
    return workerEnv.getWorkerId();
  }

  // todo: find a better OOP way of doing this!
  public <T> BatchSourceTSet<T> createBatchSource(SourceFunc<T> source, int parallelism) {
    if (operationMode == OperationMode.STREAMING) {
      throw new RuntimeException("Batch sources can not be created in streaming mode");
    }

    BatchSourceTSet<T> sourceT = new BatchSourceTSet<>(this, source, parallelism);
    tsetGraph.addTSet(sourceT);

    return sourceT;
  }

  public <T> StreamingSourceTSet<T> createStreamingSource(SourceFunc<T> source, int parallelism) {
    if (operationMode == OperationMode.BATCH) {
      throw new RuntimeException("Streaming sources can not be created in batch mode");
    }

    StreamingSourceTSet<T> sourceT = new StreamingSourceTSet<>(this, source, parallelism);
    tsetGraph.addTSet(sourceT);

    return sourceT;
  }

  /**
   * Runs the entire TSet graph
   */
  public void run() {
    DataFlowTaskGraph graph = tsetGraph.build();
    executeDataFlowGraph(graph, null);
  }

  /**
   * Runs a subgraph of TSets from the specified TSet
   *
   * @param leafTset leaf tset
   */
  public void run(BaseTSet leafTset) {
    DataFlowTaskGraph dataflowGraph = tsetGraph.build(leafTset);
    executeDataFlowGraph(dataflowGraph, null);
  }

  /**
   * Runs a subgraph of TSets from the specified TSet and output results as a tset
   *
   * @param leafTset leaf tset
   * @param <T> type of the output data object
   * @return output result as a data object
   */
  public <T> DataObject<T> runAndGet(BaseTSet leafTset) {
    DataFlowTaskGraph dataflowGraph = tsetGraph.build(leafTset);
    return executeDataFlowGraph(dataflowGraph, leafTset);
  }

  /**
   * execute data flow graph
   *
   * @param dataflowGraph data flow graph
   * @param outputTset output tset. If null, then no output would be returned
   * @param <T> type of the output data object
   * @return output as a data object if outputTset is not null. Else null
   */
  private <T> DataObject<T> executeDataFlowGraph(DataFlowTaskGraph dataflowGraph,
                                                 BuildableTSet outputTset) {
    ExecutionPlan executionPlan = taskExecutor.plan(dataflowGraph);

    LOG.fine(executionPlan::toString);
    LOG.fine(() -> "edges: " + dataflowGraph.getDirectedEdgesSet());
    LOG.fine(() -> "vertices: " + dataflowGraph.getTaskVertexSet());

    pushInputsToFunctions(dataflowGraph, executionPlan);
    taskExecutor.execute(dataflowGraph, executionPlan);

    // once a graph is built and executed, reset the underlying builder!
    tsetGraph.resetDfwGraphBuilder();

    if (outputTset != null) {
      return this.taskExecutor.getOutput(null, executionPlan, outputTset.getName());
    }

    return null;
  }


  public void addInput(String taskName, String key, Cacheable<?> input) {
    Map<String, Cacheable<?>> temp = tSetInputMap.getOrDefault(taskName, new HashMap<>());
    temp.put(key, input);
    tSetInputMap.put(taskName, temp);
  }

  /**
   * pushes the inputs into each task before the task execution is done
   *
   * @param executionPlan the built execution plan
   */
  private void pushInputsToFunctions(DataFlowTaskGraph graph, ExecutionPlan executionPlan) {
    for (String taskName : tSetInputMap.keySet()) {
      Map<String, Cacheable<?>> tempMap = tSetInputMap.get(taskName);
      for (String keyName : tempMap.keySet()) {
        taskExecutor.addInput(graph, executionPlan, taskName,
            keyName, tempMap.get(keyName).getDataObject());
      }
    }
  }

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
