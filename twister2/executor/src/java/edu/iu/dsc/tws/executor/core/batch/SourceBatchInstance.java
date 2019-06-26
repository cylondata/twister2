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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.OutputCollection;
import edu.iu.dsc.tws.api.task.executor.ExecutorContext;
import edu.iu.dsc.tws.api.task.executor.INodeInstance;
import edu.iu.dsc.tws.api.task.executor.IParallelOperation;
import edu.iu.dsc.tws.api.task.executor.ISync;
import edu.iu.dsc.tws.api.task.modifiers.Closable;
import edu.iu.dsc.tws.api.task.nodes.INode;
import edu.iu.dsc.tws.api.task.nodes.ISource;
import edu.iu.dsc.tws.api.task.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.executor.core.DefaultOutputCollection;
import edu.iu.dsc.tws.executor.core.TaskContextImpl;

public class SourceBatchInstance implements INodeInstance, ISync {
  private static final Logger LOG = Logger.getLogger(SourceBatchInstance.class.getName());

  /**
   * The actual task executing
   */
  private ISource batchTask;

  /**
   * Output will go throuh a single queue
   */
  private BlockingQueue<IMessage> outBatchQueue;

  /**
   * The configuration
   */
  private Config config;

  /**
   * The output collection to be used
   */
  private OutputCollection outputBatchCollection;

  /**
   * Parallel operations
   */
  private Map<String, IParallelOperation> outBatchParOps = new HashMap<>();

  /**
   * The globally unique task id
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
   * Name of the task
   */
  private String batchTaskName;

  /**
   * Node configurations
   */
  private Map<String, Object> nodeConfigs;

  /**
   * Worker id
   */
  private int workerId;

  /**
   * State of the running instance
   */
  private InstanceState state = new InstanceState(InstanceState.INIT);

  /**
   * The task context
   */
  private TaskContextImpl taskContext;

  /**
   * The output edges
   */
  private Map<String, String> outputEdges;

  /**
   * The low watermark for queued messages
   */
  private int lowWaterMark;

  /**
   * The high water mark for messages
   */
  private int highWaterMark;

  /**
   * The task schedule
   */
  private TaskSchedulePlan taskSchedule;

  /**
   * Keep an array for iteration
   */
  private IParallelOperation[] outOpArray;

  /**
   * Keep an array out out edges for iteration
   */
  private String[] outEdgeArray;

  public SourceBatchInstance(ISource task, BlockingQueue<IMessage> outQueue,
                             Config config, String tName, int taskId,
                             int globalTaskId, int tIndex, int parallel,
                             int wId, Map<String, Object> cfgs, Map<String, String> outEdges,
                             TaskSchedulePlan taskSchedule,
                             CheckpointingClient checkpointingClient, String taskGraphName,
                             long tasksVersion) {
    this.batchTask = task;
    this.outBatchQueue = outQueue;
    this.config = config;
    this.globalTaskId = globalTaskId;
    this.taskId = taskId;
    this.batchTaskIndex = tIndex;
    this.parallelism = parallel;
    this.batchTaskName = tName;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.outputEdges = outEdges;
    this.lowWaterMark = ExecutorContext.instanceQueueLowWaterMark(config);
    this.highWaterMark = ExecutorContext.instanceQueueHighWaterMark(config);
    this.taskSchedule = taskSchedule;
  }

  public void prepare(Config cfg) {
    outputBatchCollection = new DefaultOutputCollection(outBatchQueue);

    taskContext = new TaskContextImpl(batchTaskIndex, taskId, globalTaskId, batchTaskName,
        parallelism, workerId, outputBatchCollection, nodeConfigs, outputEdges, taskSchedule);
    batchTask.prepare(cfg, taskContext);

    /// we will use this array for iteration
    this.outOpArray = new IParallelOperation[outBatchParOps.size()];
    int index = 0;
    for (Map.Entry<String, IParallelOperation> e : outBatchParOps.entrySet()) {
      this.outOpArray[index++] = e.getValue();
    }

    this.outEdgeArray = new String[outputEdges.size()];
    index = 0;
    for (String e : outputEdges.keySet()) {
      this.outEdgeArray[index++] = e;
    }
  }

  /**
   * Execution Method calls the SourceTasks run method to get context
   **/
  public boolean execute() {
    // we started the execution
    if (state.isEqual(InstanceState.INIT)) {
      state.addState(InstanceState.EXECUTING);
    }

    if (state.isSet(InstanceState.EXECUTING) && state.isNotSet(InstanceState.EXECUTION_DONE)) {
      // we loop until low watermark is reached or all edges are done
      while (outBatchQueue.size() < lowWaterMark) {
        // if we are in executing state we can run
        batchTask.execute();

        // if all the edges are done
        if (taskContext.allEdgedFinished()) {
          state.addState(InstanceState.EXECUTION_DONE);
          break;
        }
      }
    }

    // now check the output queue
    while (!outBatchQueue.isEmpty()) {
      IMessage message = outBatchQueue.peek();
      if (message != null) {
        String edge = message.edge();
        IParallelOperation op = outBatchParOps.get(edge);
        if (op.send(globalTaskId, message, 0)) {
          outBatchQueue.poll();
        } else {
          // no point in progressing further
          break;
        }
      }
    }

    // if execution is done and outqueue is emput, we have put everything to communication
    if (state.isSet(InstanceState.EXECUTION_DONE) && outBatchQueue.isEmpty()
        && state.isNotSet(InstanceState.OUT_COMPLETE)) {
      for (IParallelOperation op : outBatchParOps.values()) {
        op.finish(globalTaskId);
      }
      state.addState(InstanceState.OUT_COMPLETE);
    }

    // lets progress the communication
    boolean needsFurther = progressCommunication();
    // after we have put everything to communication and no progress is required, lets finish
    if (state.isSet(InstanceState.OUT_COMPLETE)) {
      state.addState(InstanceState.SENDING_DONE);
    }

    boolean equal = state.isEqual(InstanceState.FINISH);
    return !equal;
  }

  public boolean sync(String edge, byte[] value) {
    state.addState(InstanceState.SYNCED);
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
  public void reset() {
    if (batchTask instanceof Closable) {
      ((Closable) batchTask).reset();
    }
    taskContext.reset();
    state = new InstanceState(InstanceState.INIT);
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
    for (int i = 0; i < outOpArray.length; i++) {
      if (outOpArray[i].progress()) {
        allDone = false;
      }
    }
    return !allDone;
  }

  @Override
  public boolean isComplete() {
    boolean complete = true;
    for (int i = 0; i < outOpArray.length; i++) {
      if (!outOpArray[i].isComplete()) {
        complete = false;
      }
    }
    return complete;
  }


  public BlockingQueue<IMessage> getOutQueue() {
    return outBatchQueue;
  }

  public void registerOutParallelOperation(String edge, IParallelOperation op) {
    outBatchParOps.put(edge, op);
  }

  public int getWorkerId() {
    return workerId;
  }
}
