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
package edu.iu.dsc.tws.executor.core.streaming.window;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.executor.api.IWindowInstance;
import edu.iu.dsc.tws.executor.core.TaskContextImpl;
import edu.iu.dsc.tws.task.api.Closable;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.window.IWindowCompute;
import edu.iu.dsc.tws.task.api.window.manage.WindowManager;
import edu.iu.dsc.tws.task.api.window.policy.WindowingPolicy;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class SinkStreamingWindowingInstance implements INodeInstance, IWindowInstance {

  private static final Logger LOG = Logger
      .getLogger(SinkStreamingWindowingInstance.class.getName());

  /**
   * The actual windowing streamingTask executing
   */
  private IWindowCompute streamingWindowTask;

  /**
   * All the inputs will come through a single queue, otherwise we need to look
   * at different queues for messages
   */
  private BlockingQueue<IMessage> streamingInQueue;

  /**
   * Inward parallel operations
   */
  private Map<String, IParallelOperation> streamingInParOps = new HashMap<>();

  /**
   * The configuration
   */
  private Config config;

  /**
   * The globally unique streamingTask id
   */
  private int streamingTaskId;

  /**
   * Task index that goes from 0 to parallism - 1
   */
  private int streamingTaskIndex;

  /**
   * Number of parallel tasks
   */
  private int parallelism;

  /**
   * Name of the streamingTask
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
   * The input edges
   */
  private Map<String, String> inEdges;
  private TaskSchedulePlan taskSchedulePlan;

  private WindowingPolicy windowingPolicy;

  private int windowSize = 0;

  private boolean policyApplied = false;

  private List<IMessage<?>> messageList = new ArrayList<>(windowSize);

  private WindowManager<IMessage<?>> windowManager;

  private ReentrantLock lock = new ReentrantLock();


  public SinkStreamingWindowingInstance(IWindowCompute streamingWindowTask,
                                        BlockingQueue<IMessage> streamingInQueue, Config config,
                                        String tName, int tId, int tIndex, int parallel, int wId,
                                        Map<String, Object> cfgs, Map<String, String> inEdges,
                                        TaskSchedulePlan taskSchedulePlan,
                                        WindowingPolicy winPolicy) {
    this.streamingWindowTask = streamingWindowTask;
    this.streamingInQueue = streamingInQueue;
    this.config = config;
    this.streamingTaskId = tId;
    this.streamingTaskIndex = tIndex;
    this.parallelism = parallel;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.taskName = tName;
    this.inEdges = inEdges;
    this.taskSchedulePlan = taskSchedulePlan;
    this.windowingPolicy = winPolicy;
    this.windowManager = new WindowManager<>(this.windowingPolicy);
    this.windowSize = this.windowingPolicy.getCount().value;
    this.messageList = new ArrayList<>(this.windowSize);
  }

  public SinkStreamingWindowingInstance(IWindowCompute streamingWindowTask,
                                        BlockingQueue<IMessage> streamingInQueue, Config config,
                                        String tName, int tId, int tIndex, int parallel, int wId,
                                        Map<String, Object> cfgs, Map<String, String> inEdges,
                                        TaskSchedulePlan taskSchedulePlan) {
    this.streamingWindowTask = streamingWindowTask;
    this.streamingInQueue = streamingInQueue;
    this.config = config;
    this.streamingTaskId = tId;
    this.streamingTaskIndex = tIndex;
    this.parallelism = parallel;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.taskName = tName;
    this.inEdges = inEdges;
    this.taskSchedulePlan = taskSchedulePlan;
  }

  @Override
  public boolean execute() {

    while (!streamingInQueue.isEmpty()) {
      IMessage m = streamingInQueue.poll();
      if (m != null) {
        this.streamingWindowTask.execute(m);
      }
    }

    for (Map.Entry<String, IParallelOperation> e : streamingInParOps.entrySet()) {
      e.getValue().progress();
    }

    return true;
  }

  @Override
  public void prepare(Config cfg) {
    streamingWindowTask.prepare(cfg, new TaskContextImpl(streamingTaskIndex, streamingTaskId,
        taskName, parallelism, workerId, nodeConfigs, inEdges, taskSchedulePlan));
  }

  @Override
  public INode getNode() {
    return streamingWindowTask;
  }

  @Override
  public void close() {
    if (streamingWindowTask instanceof Closable) {
      ((Closable) streamingWindowTask).close();
    }
  }

  public void registerInParallelOperation(String edge, IParallelOperation op) {
    streamingInParOps.put(edge, op);
  }

  public BlockingQueue<IMessage> getstreamingInQueue() {
    return streamingInQueue;
  }

  public WindowingPolicy getWindowingPolicy() {
    return windowingPolicy;
  }

  public void setWindowingPolicy(WindowingPolicy windowingPolicy) {
    this.windowingPolicy = windowingPolicy;
  }

  @Override
  public void initializePolicy() {
    if (this.windowingPolicy == null) {
      throw new RuntimeException("Windowing Policy Not defined");
    } else {
      //
    }
  }
}
