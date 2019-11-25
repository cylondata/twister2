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

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.sets.TupleTSet;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingConfigurations;
import edu.iu.dsc.tws.task.impl.TaskExecutor;
import edu.iu.dsc.tws.tset.TBaseGraph;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

/**
 * Entry point to tset operations. This is a singleton which initializes as
 * {@link BatchTSetEnvironment} or {@link StreamingTSetEnvironment}
 */
public abstract class TSetEnvironment {
  private static final Logger LOG = Logger.getLogger(TSetEnvironment.class.getName());

  private transient WorkerEnvironment workerEnv;
  private transient TBaseGraph tBaseGraph;
  private transient TaskExecutor taskExecutor;

  private int defaultParallelism = 1;
  private boolean isCDFW = false;

  // map (tsetID --> ( map ( input tset id --> input key)))
  private Map<String, Map<String, String>> tSetInputMap = new HashMap<>();

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

  public abstract <T> BaseTSet<T> createSource(String name, SourceFunc<T> source, int parallelism);

  public abstract <K, V> TupleTSet<K, V> createKeyedSource(SourceFunc<Tuple<K, V>> source,
                                                           int parallelism);

  public abstract <K, V> TupleTSet<K, V> createKeyedSource(String name,
                                                           SourceFunc<Tuple<K, V>> source,
                                                           int parallelism);

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

  /**
   * Checks if checkpointing is enabled
   *
   * @return bool
   */
  public boolean isCheckpointingEnabled() {
    return CheckpointingConfigurations.isCheckpointingEnabled(this.getConfig())
        && this instanceof CheckpointingTSetEnv;
  }

  /**
   * Adds a {@link edu.iu.dsc.tws.api.tset.sets.TSet} to another
   * {@link edu.iu.dsc.tws.api.tset.sets.TSet} as an input that will be identified by the inputKey
   *
   * @param tSetID      TSet ID
   * @param inputTSetID input TSet ID
   * @param inputKey    key given to the input TSet
   */
  public void addInput(String tSetID, String inputTSetID, String inputKey) {
    if (tSetInputMap.containsKey(tSetID)) {
      tSetInputMap.get(tSetID).put(inputTSetID, inputKey);
    } else {
      Map<String, String> temp = new HashMap<>();
      temp.put(inputTSetID, inputKey);
      tSetInputMap.put(tSetID, temp);
    }
  }

  /**
   * Returns the map of inputs of a particular {@link edu.iu.dsc.tws.api.tset.sets.TSet}
   *
   * @param tSetID TSet ID
   * @return map of inputs that maps inputTSetDD --> inputKey
   */
  public Map<String, String> getInputs(String tSetID) {
    return tSetInputMap.getOrDefault(tSetID, new HashMap<>());
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

  /**
   * Sets {@link TBaseGraph} based on the {@link OperationMode}
   *
   * @param tBaseGraph TBase graph
   */
  protected void settBaseGraph(TBaseGraph tBaseGraph) {
    this.tBaseGraph = tBaseGraph;
  }

  /**
   * Executes data flow graph wrapped by a {@link BuildContext}
   *
   * @param buildContext data flow graph wrapped by {@link BuildContext}
   */
  protected void executeBuildContext(BuildContext buildContext) {
    // build the context which will create compute graph and execution plan
    buildContext.build(taskExecutor);

    LOG.fine(buildContext.getComputeGraph()::toString);
    LOG.fine(() -> "edges: " + buildContext.getComputeGraph().getDirectedEdgesSet());
    LOG.fine(() -> "vertices: " + buildContext.getComputeGraph().getTaskVertexSet());

    taskExecutor.execute(buildContext.getComputeGraph(), buildContext.getExecutionPlan());
  }

  // TSet graph for classes that extends TSetEnvironment
  TBaseGraph getTSetGraph() {
    return tBaseGraph;
  }

  // task executor for classes that extends TSetEnvironment
  TaskExecutor getTaskExecutor() {
    return taskExecutor;
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
}
