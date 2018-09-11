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
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.executor.core.DefaultOutputCollection;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.OutputCollection;
import edu.iu.dsc.tws.task.api.TaskContext;

public class SourceBatchInstance implements INodeInstance {

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
  private int batchTaskId;

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
  private Set<String> outputEdges;

  /**
   * The low watermark for queued messages
   */
  private int lowWaterMark;

  /**
   * The high water mark for messages
   */
  private int highWaterMark;

  public SourceBatchInstance(ISource task, BlockingQueue<IMessage> outQueue,
                             Config config, String tName, int tId, int tIndex, int parallel,
                             int wId, Map<String, Object> cfgs, Set<String> outEdges) {
    this.batchTask = task;
    this.outBatchQueue = outQueue;
    this.config = config;
    this.batchTaskId = tId;
    this.batchTaskIndex = tIndex;
    this.parallelism = parallel;
    this.batchTaskName = tName;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.outputEdges = outEdges;
    this.lowWaterMark = ExecutorContext.instanceQueueLowWaterMark(config);
    this.highWaterMark = ExecutorContext.instanceQueueHighWaterMark(config);
  }

  public void prepare() {
    outputBatchCollection = new DefaultOutputCollection(outBatchQueue);

    taskContext = new TaskContext(batchTaskIndex, batchTaskId, batchTaskName,
        parallelism, workerId, outputBatchCollection, nodeConfigs);
    batchTask.prepare(config, taskContext);
  }

  /**
   * Execution Method calls the SourceTasks run method to get context
   **/
  public boolean execute() {
    // we started the execution
    if (state.isEqual(InstanceState.INIT)) {
      state.set(InstanceState.EXECUTING);
    }

    if (batchTask != null) {
      // if we are in executing state we can run
      if (state.isSet(InstanceState.EXECUTING) && state.isNotSet(InstanceState.EXECUTION_DONE)
          && outBatchQueue.size() < lowWaterMark) {
        batchTask.execute();
      }

      // now check the context
      boolean isDone = true;
      for (String e : outputEdges) {
        if (!taskContext.isDone(e)) {
          // we are done with execution
          isDone = false;
          break;
        }
      }
      // if all the edges are done
      if (isDone) {
        state.set(InstanceState.EXECUTION_DONE);
      }

      // now check the output queue
      while (!outBatchQueue.isEmpty()) {
        IMessage message = outBatchQueue.peek();
        if (message != null) {
          String edge = message.edge();
          IParallelOperation op = outBatchParOps.get(edge);
          if (op.send(batchTaskId, message, 0)) {
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
          op.finish(batchTaskId);
        }
        state.set(InstanceState.OUT_COMPLETE);
      }
    }

    // lets progress the communication
    boolean needsFurther = communicationProgress();
    // after we have put everything to communication and no progress is required, lets finish
    if (state.isSet(InstanceState.OUT_COMPLETE) && !needsFurther) {
      state.set(InstanceState.SENDING_DONE);
    }

    return !state.isEqual(InstanceState.FINISH);
  }

  @Override
  public int getId() {
    return batchTaskId;
  }

  @Override
  public INode getNode() {
    return batchTask;
  }

  /**
   * Progress the communication and return weather we need to further progress
   * @return true if further progress is needed
   */
  public boolean communicationProgress() {
    boolean allDone = true;
    for (Map.Entry<String, IParallelOperation> e : outBatchParOps.entrySet()) {
      if (e.getValue().progress()) {
        allDone = false;
      }
    }
    return !allDone;
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
