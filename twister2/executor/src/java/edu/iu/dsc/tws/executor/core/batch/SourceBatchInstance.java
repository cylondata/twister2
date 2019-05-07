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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.executor.api.ISync;
import edu.iu.dsc.tws.executor.core.DefaultOutputCollection;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.executor.core.TaskContextImpl;
import edu.iu.dsc.tws.task.api.Closable;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.OutputCollection;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class SourceBatchInstance implements INodeInstance, ISync {

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
  private TaskContext taskContext;

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

  public SourceBatchInstance(ISource task, BlockingQueue<IMessage> outQueue,
                             Config config, String tName, int taskId,
                             int globalTaskId, int tIndex, int parallel,
                             int wId, Map<String, Object> cfgs, Map<String, String> outEdges,
                             TaskSchedulePlan taskSchedule) {
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
  }

  /**
   * Execution Method calls the SourceTasks run method to get context
   **/
  public boolean execute() {
    // we started the execution
    if (state.isEqual(InstanceState.INIT)) {
      state.addState(InstanceState.EXECUTING);
    }

    if (batchTask != null) {
      // if we are in executing state we can run
      if (state.isSet(InstanceState.EXECUTING) && state.isNotSet(InstanceState.EXECUTION_DONE)
          && outBatchQueue.size() < lowWaterMark) {
        batchTask.execute();
      }

      // now check the context
      boolean isDone = true;
      for (String e : outputEdges.keySet()) {
        if (!taskContext.isDone(e)) {
          // we are done with execution
          isDone = false;
          break;
        }
      }
      // if all the edges are done
      if (isDone) {
        state.addState(InstanceState.EXECUTION_DONE);
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
    }

    // lets progress the communication
    boolean needsFurther = progressCommunication();
    // after we have put everything to communication and no progress is required, lets finish
    if (state.isSet(InstanceState.OUT_COMPLETE)) {
      state.addState(InstanceState.SENDING_DONE);
    }

    return !state.isEqual(InstanceState.FINISH);
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
      ((Closable) batchTask).refresh();
    }
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
    for (Map.Entry<String, IParallelOperation> e : outBatchParOps.entrySet()) {
      if (e.getValue().progress()) {
        allDone = false;
      }
    }
    return !allDone;
  }

  @Override
  public boolean isComplete() {
    boolean complete = true;
    for (Map.Entry<String, IParallelOperation> e : outBatchParOps.entrySet()) {
      if (!e.getValue().isComplete()) {
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
