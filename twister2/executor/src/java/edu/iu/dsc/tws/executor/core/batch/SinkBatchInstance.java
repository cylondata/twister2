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
package edu.iu.dsc.tws.executor.core.batch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.api.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.executor.INodeInstance;
import edu.iu.dsc.tws.api.task.executor.IParallelOperation;
import edu.iu.dsc.tws.api.task.executor.ISync;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.task.modifiers.Closable;
import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.task.nodes.INode;
import edu.iu.dsc.tws.api.task.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.executor.core.TaskContextImpl;

public class SinkBatchInstance implements INodeInstance, ISync {
  /**
   * The actual batchTask executing
   */
  private ICompute batchTask;

  /**
   * All the inputs will come through a single queue, otherwise we need to look
   * at different queues for messages
   */
  private BlockingQueue<IMessage> batchInQueue;

  /**
   * Inward parallel operations
   */
  private Map<String, IParallelOperation> batchInParOps = new HashMap<>();

  /**
   * The configuration
   */
  private Config config;

  /**
   * The globally unique batchTask id
   */
  private int globalTaskId;

  /**
   * Task id
   */
  private int taskId;

  /**
   * Task index that goes from 0 to parallism - 1
   */
  private int batchTaskIndex;

  /**
   * Number of parallel tasks
   */
  private int parallelism;

  /**
   * Name of the batchTask
   */
  private String taskName;

  /**
   * Node configurations
   */
  private Map<String, Object> nodeConfigs;

  /**
   * The worker id
   */
  private int workerId;

  /**
   * Execution state of the instance
   */
  private InstanceState state = new InstanceState(InstanceState.INIT);

  /**
   * the incoming edges
   */
  private Map<String, Set<String>> inputEdges;

  /**
   * Task schedule plan contains information about whole topology. This will be passed to
   * {@link TaskContext} to expose necessary information
   */
  private TaskSchedulePlan taskSchedule;

  /**
   * Keep track of syncs received
   */
  private Set<String> syncReceived = new HashSet<>();
  private TaskContextImpl context;

  /**
   * Keep an array for iteration
   */
  private IParallelOperation[] intOpArray;

  /**
   * Keep an array out out edges for iteration
   */
  private String[] inEdgeArray;

  public SinkBatchInstance(ICompute batchTask, BlockingQueue<IMessage> batchInQueue, Config config,
                           String tName, int taskId, int globalTaskId,
                           int tIndex, int parallel, int wId,
                           Map<String, Object> cfgs, Map<String, Set<String>> inEdges,
                           TaskSchedulePlan taskSchedule, CheckpointingClient checkpointingClient,
                           String taskGraphName, Long taskVersion) {
    this.batchTask = batchTask;
    this.batchInQueue = batchInQueue;
    this.config = config;
    this.globalTaskId = globalTaskId;
    this.taskId = taskId;
    this.batchTaskIndex = tIndex;
    this.parallelism = parallel;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.taskName = tName;
    this.inputEdges = inEdges;
    this.taskSchedule = taskSchedule;
  }

  public void reset() {
    this.context.reset();
    state = new InstanceState(InstanceState.INIT);
    if (batchTask instanceof Closable) {
      ((Closable) batchTask).reset();
    }
  }

  public void prepare(Config cfg) {
    context = new TaskContextImpl(batchTaskIndex, taskId, globalTaskId, taskName,
        parallelism, workerId, nodeConfigs, inputEdges, taskSchedule, OperationMode.BATCH);
    batchTask.prepare(cfg, context);

    /// we will use this array for iteration
    this.intOpArray = new IParallelOperation[batchInParOps.size()];
    int index = 0;
    for (Map.Entry<String, IParallelOperation> e : batchInParOps.entrySet()) {
      this.intOpArray[index++] = e.getValue();
    }

    this.inEdgeArray = new String[inputEdges.size()];
    index = 0;
    for (String e : inputEdges.keySet()) {
      this.inEdgeArray[index++] = e;
    }
  }

  public boolean execute() {
    // we started the execution
    // if execution has not yet finished
    if (state.isSet(InstanceState.INIT) && state.isNotSet(InstanceState.EXECUTION_DONE)) {
      while (!batchInQueue.isEmpty()) {
        IMessage m = batchInQueue.poll();
        batchTask.execute(m);
        state.addState(InstanceState.EXECUTING);
      }

      // lets progress the communication
      boolean needsFurther = progressCommunication();

      // we don't have incoming and our inqueue in empty
      if ((state.isSet(InstanceState.EXECUTING) && batchInQueue.isEmpty())
          || (batchInQueue.isEmpty() && state.isSet(InstanceState.SYNCED))) {
        state.addState(InstanceState.EXECUTION_DONE);
      }
    }

    // we only need the execution done for now
    return !state.isSet(InstanceState.EXECUTION_DONE | InstanceState.SYNCED);
  }

  public boolean sync(String edge, byte[] value) {
    syncReceived.add(edge);
    if (syncReceived.equals(batchInParOps.keySet())) {
      state.addState(InstanceState.SYNCED);
      syncReceived.clear();
    }
    return true;
  }

  @Override
  public int getId() {
    return globalTaskId;
  }

  @Override
  public INode getNode() {
    return batchTask;
  }

  @Override
  public void close() {
    if (batchTask instanceof Closable) {
      ((Closable) batchTask).close();
    }
  }

  /**
   * Progress the communication and return weather we need to further progress
   *
   * @return true if further progress is needed
   */
  public boolean progressCommunication() {
    boolean allDone = true;
    for (int i = 0; i < intOpArray.length; i++) {
      if (intOpArray[i].progress()) {
        allDone = false;
      }
    }
    return !allDone;
  }

  @Override
  public boolean isComplete() {
    boolean complete = true;
    for (int i = 0; i < intOpArray.length; i++) {
      if (!intOpArray[i].isComplete()) {
        complete = false;
      }
    }
    return complete;
  }

  public void registerInParallelOperation(String edge, IParallelOperation op) {
    batchInParOps.put(edge, op);
  }

  public BlockingQueue<IMessage> getBatchInQueue() {
    return batchInQueue;
  }

  public int getGlobalTaskId() {
    return globalTaskId;
  }

  public int getBatchTaskIndex() {
    return batchTaskIndex;
  }

  public int getParallelism() {
    return parallelism;
  }

  public String getTaskName() {
    return taskName;
  }

  public int getWorkerId() {
    return workerId;
  }
}
