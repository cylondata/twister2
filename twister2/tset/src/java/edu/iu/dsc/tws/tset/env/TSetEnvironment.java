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
package edu.iu.dsc.tws.tset.env;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.Cacheable;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.dataset.EmptyDataObject;
import edu.iu.dsc.tws.task.impl.TaskExecutor;
import edu.iu.dsc.tws.tset.TBaseGraph;
import edu.iu.dsc.tws.tset.sets.BaseTSet;
import edu.iu.dsc.tws.tset.sets.BuildableTSet;

/**
 * Entry point to tset operations. This is a singleton which initializes as
 * {@link BatchTSetEnvironment} or {@link StreamingTSetEnvironment}
 */
public abstract class TSetEnvironment {
  private static final Logger LOG = Logger.getLogger(TSetEnvironment.class.getName());

  private WorkerEnvironment workerEnv;
  private TBaseGraph tBaseGraph;
  private TaskExecutor taskExecutor;
  //  private ComputeGraph itergraph;
//  private ExecutionPlan iterexecutionPlan;
  private int defaultParallelism = 1;
  private boolean isCDFW = false;

  private Map<String, Map<String, Cacheable<?>>> tSetInputMap = new HashMap<>();

  private static volatile TSetEnvironment thisTSetEnv;

  protected TSetEnvironment(WorkerEnvironment wEnv) {
    this.workerEnv = wEnv;

    this.tBaseGraph = new TBaseGraph(getOperationMode());

    // can not use task env at the moment because it does not support graph builder API
    this.taskExecutor = new TaskExecutor(workerEnv);
  }

  /**
   * Used to construct the TSet environment when in the connected data flow mode.
   */
  protected TSetEnvironment() {
    this.isCDFW = true;
    this.tBaseGraph = new TBaseGraph(getOperationMode());
  }

  public abstract OperationMode getOperationMode();

  public abstract <T> BaseTSet<T> createSource(SourceFunc<T> source, int parallelism);

  /**
   * Returns the tset graph
   *
   * @return tset graph
   */
  public TBaseGraph getGraph() {
    return tBaseGraph;
  }

  /**
   * Overrides the default parallelism. Default is 1
   *
   * @param newDefaultParallelism new parallelism
   */
  public void setDefaultParallelism(int newDefaultParallelism) {
    this.defaultParallelism = newDefaultParallelism;
  }

  /**
   * Default parallelism
   *
   * @return default parallelism
   */
  public int getDefaultParallelism() {
    return defaultParallelism;
  }

  /**
   * returns the config object passed on to the iWorker Config
   *
   * @return config
   */
  public Config getConfig() {
    return workerEnv.getConfig();
  }

  /**
   * Running worker ID
   *
   * @return workerID
   */
  public int getWorkerID() {
    return workerEnv.getWorkerId();
  }

  TBaseGraph getTSetGraph() {
    return tBaseGraph;
  }

  /**
   * execute data flow graph
   *
   * @param <T> type of the output data object
   * @param buildContext data flow graph
   * @param outputTset output tset. If null, then no output would be returned
   * @return output as a data object if outputTset is not null. Else null
   */
  protected <T> DataObject<T> executeBuildContext(BuildContext buildContext,
                                                  BuildableTSet outputTset, boolean isIterative) {
//    ExecutionPlan executionPlan = null;
//    if (isIterative && iterexecutionPlan != null) {
//      executionPlan = iterexecutionPlan;
//    } else {
//      executionPlan = taskExecutor.plan(dataflowGraph);
//      if (isIterative) {
//        iterexecutionPlan = executionPlan;
//        itergraph = dataflowGraph;
//      }
//    }

    // build the context which will create compute graph and execution plan
    buildContext.build(taskExecutor);

    LOG.fine(buildContext.getComputeGraph()::toString);
    LOG.fine(() -> "edges: " + buildContext.getComputeGraph().getDirectedEdgesSet());
    LOG.fine(() -> "vertices: " + buildContext.getComputeGraph().getTaskVertexSet());

    taskExecutor.execute(buildContext.getComputeGraph(), buildContext.getExecutionPlan());

//    pushInputsToFunctions(buildContext, buildContext.);
//    if (isIterative) {
//      taskExecutor.itrExecute(buildContext, executionPlan);
//    } else {
//      taskExecutor.execute(buildContext, executionPlan);
//
//      // once a graph is built and executed, reset the underlying builder!
//      tBaseGraph.resetDfwGraphBuilder();
//    }

    // output tset alone does not guarantees that there will be an output available.
    // Example: if the output is done after a reduce, parallelism(output tset) = 1. Then only
    // executor 1 would have an output to get.
//    if (outputTset != null && executionPlan.isNodeAvailable(outputTset.getId())) {
//      return this.taskExecutor.getOutput(null, executionPlan, outputTset.getId());
//    }

    // if there is no output, an empty data object needs to be returned!
    return new EmptyDataObject<>();
  }

  protected <T> DataObject<T> evalBuildCtx(BuildContext buildContext) {
    // build the context which will create compute graph and execution plan
    buildContext.build(taskExecutor);

    LOG.fine(buildContext.getComputeGraph()::toString);
    LOG.fine(() -> "edges: " + buildContext.getComputeGraph().getDirectedEdgesSet());
    LOG.fine(() -> "vertices: " + buildContext.getComputeGraph().getTaskVertexSet());

    taskExecutor.itrExecute(buildContext.getComputeGraph(), buildContext.getExecutionPlan());

    return new EmptyDataObject<>();
  }

  protected void finishEval(BuildContext ctx) {
    taskExecutor.;
    taskExecutor.closeExecution(ctx.getComputeGraph(), ctx.getExecutionPlan());
  }

  /**
   * Adds inputs to tasks
   *
   * @param taskName task name
   * @param key identifier/ key for the input
   * @param input a cacheable object which returns a {@link DataObject}
   */
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
  protected void pushInputsToFunctions(ComputeGraph graph, ExecutionPlan executionPlan) {
    // todo: fix this!
//    for (String taskName : tSetInputMap.keySet()) {
//      Map<String, Cacheable<?>> tempMap = tSetInputMap.get(taskName);
//      for (String keyName : tempMap.keySet()) {
//        taskExecutor.addInput(graph, executionPlan, taskName,
//            keyName, tempMap.get(keyName).getDataObject());
//      }
//    }
  }

  // TSetEnvironment singleton initialization
  private static TSetEnvironment init(WorkerEnvironment wEnv, OperationMode opMode) {
    if (thisTSetEnv == null) {
      synchronized (TSetEnvironment.class) {
        if (thisTSetEnv == null) {
          if (opMode == OperationMode.BATCH) {
            thisTSetEnv = new BatchTSetEnvironment(wEnv);
          } else { // streaming
            thisTSetEnv = new StreamingTSetEnvironment(wEnv);
          }
        }
      }
    }

    return thisTSetEnv;
  }

  /**
   * initialize the Tset environment in batch mode
   *
   * @param wEnv worker environment
   * @return tset environment for batch operation
   */
  public static BatchTSetEnvironment initBatch(WorkerEnvironment wEnv) {
    return (BatchTSetEnvironment) init(wEnv, OperationMode.BATCH);
  }

  /**
   * initialize the Tset environment in streaming mode
   *
   * @param wEnv worker environment
   * @return tset environment for streaming operation
   */
  public static StreamingTSetEnvironment initStreaming(WorkerEnvironment wEnv) {
    return (StreamingTSetEnvironment) init(wEnv, OperationMode.STREAMING);
  }
}
