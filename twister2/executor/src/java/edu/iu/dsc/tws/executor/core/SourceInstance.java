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
package edu.iu.dsc.tws.executor.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.executor.api.DefaultOutputCollection;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.api.INodeInstanceListener;
import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.executor.comm.tasks.batch.SourceBatchTask;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.OutputCollection;
import edu.iu.dsc.tws.task.api.SourceTask;
import edu.iu.dsc.tws.task.api.TaskContext;

public class SourceInstance implements INodeInstance, INodeInstanceListener {
  /**
   * The actual task executing
   */
  private ISource task;

  /**
   * Output will go throuh a single queue
   */
  private BlockingQueue<IMessage> outQueue;

  /**
   * The configuration
   */
  private Config config;

  /**
   * The output collection to be used
   */
  private OutputCollection outputCollection;

  /**
   * Parallel operations
   */
  private Map<String, IParallelOperation> outParOps = new HashMap<>();

  /**
   * The globally unique task id
   */
  private int taskId;

  /**
   * Task index that goes from 0 to parallism - 1
   */
  private int taskIndex;

  /**
   * Number of parallel tasks
   */
  private int parallelism;

  /**
   * Name of the task
   */
  private String taskName;

  /**
   * Node configurations
   */
  private Map<String, Object> nodeConfigs;

  /**
   * Worker id
   */
  private int workerId;

  /**
   * For batch tasks: identifying the final message
   **/

  private boolean isDone;

  private boolean isFinalTask;

  private int callBackCount = 0;

  private int count = 0;


  public SourceInstance(ISource task, BlockingQueue<IMessage> outQueue, Config config, String tName,
                        int tId, int tIndex, int parallel, int wId, Map<String, Object> cfgs) {
    this.task = task;
    this.outQueue = outQueue;
    this.config = config;
    this.taskId = tId;
    this.taskIndex = tIndex;
    this.parallelism = parallel;
    this.taskName = tName;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
  }

  public SourceInstance(ISource task, BlockingQueue<IMessage> outQueue, Config config, String tName,
                        int tId, int tIndex, int parallel, int wId,
                        Map<String, Object> cfgs, boolean isDone) {
    this.task = task;
    this.outQueue = outQueue;
    this.config = config;
    this.taskId = tId;
    this.taskIndex = tIndex;
    this.parallelism = parallel;
    this.taskName = tName;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.isDone = isDone;
  }

  public void prepare() {
    outputCollection = new DefaultOutputCollection(outQueue);

    task.prepare(config, new TaskContext(taskIndex, taskId, taskName,
        parallelism, workerId, outputCollection, nodeConfigs));
  }

  @Override
  public void interrupt() {

  }

  /**
   * Execution Method calls the SourceTasks run method to get context
   **/
  public boolean execute() {

    task.run();

    if (task instanceof SourceBatchTask) {
      SourceBatchTask sourceBatchTask = (SourceBatchTask) task;
      sourceBatchTask.interrupt();
      TaskContext context = sourceBatchTask.getSourceTaskContextListener()
          .getInstanceBatchContextMap().get(sourceBatchTask);
      this.isDone = context.isDone();
    }
    // now check the output queue
    while (!outQueue.isEmpty()) {

      if (task instanceof SourceBatchTask) {
        System.out.println("Task Type : " + task.getClass().getName());
        boolean isOQEmpty = outQueue.isEmpty();
        if (!(this.isDone && isOQEmpty)) {
          IMessage message = outQueue.poll();
          if (message != null) {
            String edge = message.edge();
            IParallelOperation op = outParOps.get(edge);
            while (!op.send(taskId, message, 0)) {
              //
            }
          }
        }
      }

      if (task instanceof SourceTask) {
        System.out.println("Task Type : " + task.getClass().getName());
        IMessage message = outQueue.poll();
        if (message != null) {
          String edge = message.edge();
          System.out.println("Edge Name : " + edge);
          IParallelOperation op = outParOps.get(edge);
          System.out.println("Para op : " + op.toString());
          /*System.out.println("Para Op : " + op.getClass().getName() + ", taskId : " + taskId
              + ", Message : " + message.getContent());*/
          op.send(taskId, message, 0);
        }
      }
    }

    for (Map.Entry<String, IParallelOperation> e : outParOps.entrySet()) {
      e.getValue().progress();
    }

    return true;
  }

  public BlockingQueue<IMessage> getOutQueue() {
    return outQueue;
  }

  public void registerOutParallelOperation(String edge, IParallelOperation op) {
    outParOps.put(edge, op);
  }

  @Override
  public boolean OnDone() {
    return isDone;
  }

  @Override
  public boolean onStart() {
    return false;
  }

  @Override
  public boolean onStop() {
    return false;
  }
}
