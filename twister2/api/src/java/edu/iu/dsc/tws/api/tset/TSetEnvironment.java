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

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.task.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.fn.Source;
import edu.iu.dsc.tws.api.tset.link.BuildableTLink;
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
  private Map<String, Map<String, Cacheable<?>>> inputMap = new HashMap<>();

  private static volatile TSetEnvironment thisTSetEnv;
  private static int taskGraphCount = 0;

  private TSetEnvironment(WorkerEnvironment wEnv, OperationMode opMode) {
    this.workerEnv = wEnv;
    this.operationMode = opMode;

    this.tsetGraph = new TSetGraph(this, opMode);
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

  public int getWorkerID(){
    return workerEnv.getWorkerId();
  }

  // todo: find a better OOP way of doing this!
  public <T> BatchSourceTSet<T> createBatchSource(Source<T> source, int parallelism) {
    if (operationMode == OperationMode.STREAMING) {
      throw new RuntimeException("Batch sources can not be created in streaming mode");
    }

    BatchSourceTSet<T> sourceT = new BatchSourceTSet<>(this, source, parallelism);
    tsetGraph.addTSet(sourceT);

    return sourceT;
  }

  public <T> StreamingSourceTSet<T> createStreamingSource(Source<T> source, int parallelism) {
    if (operationMode == OperationMode.BATCH) {
      throw new RuntimeException("Streaming sources can not be created in batch mode");
    }

    StreamingSourceTSet<T> sourceT = new StreamingSourceTSet<>(this, source, parallelism);
    tsetGraph.addTSet(sourceT);

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

  public void executeTSet(BaseTSet leafTset) {
    List<BuildableTLink> linksPlan = new ArrayList<>();
    List<BuildableTSet> setsPlan = new ArrayList<>();

    invertedBFS(leafTset, linksPlan, setsPlan);

    LOG.info("Node build plan: " + setsPlan);
    buildTSets(setsPlan);

    LOG.info("Edge build plan: " + linksPlan);
    buildTLinks(linksPlan, setsPlan);

    DataFlowTaskGraph dataflowGraph = tsetGraph.getDfwGraphBuilder().build();
    dataflowGraph.setGraphName("taskgraph" + (++taskGraphCount));

    ExecutionPlan executionPlan = taskExecutor.plan(dataflowGraph);

    LOG.fine(executionPlan.toString());
    LOG.fine("edges: " + dataflowGraph.getDirectedEdgesSet());
    LOG.fine("vertices: " + dataflowGraph.getTaskVertexSet());

    taskExecutor.execute(dataflowGraph, executionPlan);

    // once a graph is built and executed, reset the underlying builder!
    tsetGraph.resetDfwGraphBuilder();
  }


  private void buildTLinks(List<BuildableTLink> tlinks, List<? extends TBase> tSets) {
    // links need to be built in order. check issue #519
    for (int i = 0; i < tlinks.size(); i++) {
      tlinks.get(tlinks.size() - i - 1).build(tsetGraph, tSets);
    }
  }

  private void buildTSets(List<BuildableTSet> tsets) {
    for (BuildableTSet baseTSet : tsets) {
      baseTSet.build(tsetGraph);
    }
  }

  private void invertedBFS(TBase s, List<BuildableTLink> links, List<BuildableTSet> sets) {
    Map<TBase, Boolean> visited = new HashMap<>();

    Deque<TBase> queue = new LinkedList<>();

    visited.put(s, true);
    queue.add(s);

    while (queue.size() != 0) {
      TBase t = queue.poll();
      if (t instanceof BuildableTLink) {
        links.add((BuildableTLink) t);
      } else if (t instanceof BuildableTSet) {
        sets.add((BuildableTSet) t);
      }

      for (TBase parent : tsetGraph.getPredecessors(t)) {
        if (visited.get(parent) == null || !visited.get(parent)) {
          visited.put(parent, true);
          queue.add(parent);
        }
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
