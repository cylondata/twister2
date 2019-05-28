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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.executor.core.DefaultOutputCollection;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.executor.core.TaskContextImpl;
import edu.iu.dsc.tws.executor.core.streaming.TaskStreamingInstance;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.window.IWindowCompute;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class TaskStreamingWindowingInstance extends TaskStreamingInstance {

  private static final Logger LOG = Logger.getLogger(TaskStreamingWindowingInstance
      .class.getName());

  /**
   * The actual windowing streamingTask executing
   */
  protected IWindowCompute streamingWindowTask;

  public TaskStreamingWindowingInstance(IWindowCompute task, BlockingQueue<IMessage> inQueue,
                                        BlockingQueue<IMessage> outQueue, Config config,
                                        String tName, int taskId, int globalTaskId, int tIndex,
                                        int parallel, int wId, Map<String, Object> cfgs,
                                        Map<String, String> inEdges, Map<String, String> outEdges,
                                        TaskSchedulePlan taskSchedule) {
    super(task, inQueue, outQueue, config, tName, taskId, globalTaskId, tIndex, parallel, wId, cfgs,
        inEdges, outEdges, taskSchedule);
    this.streamingWindowTask = task;
    this.inQueue = inQueue;
    this.config = config;
    this.globalTaskId = globalTaskId;
    this.taskId = taskId;
    this.taskIndex = tIndex;
    this.parallelism = parallel;
    this.taskName = tName;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.lowWaterMark = ExecutorContext.instanceQueueLowWaterMark(config);
    this.highWaterMark = ExecutorContext.instanceQueueHighWaterMark(config);
    this.inputEdges = inEdges;
    this.outputEdges = outEdges;
    this.taskSchedule = taskSchedule;
  }

  @Override
  public void prepare(Config cfg) {
    outputCollection = new DefaultOutputCollection(outQueue);
    this.streamingWindowTask.prepare(cfg, new TaskContextImpl(taskIndex, taskId, globalTaskId,
        taskName, parallelism, workerId, outputCollection, nodeConfigs, inputEdges, outputEdges,
        taskSchedule));
  }
}
