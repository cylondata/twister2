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
package edu.iu.dsc.tws.tsched.taskscheduler;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.tsched.batch.datalocalityaware.DataLocalityBatchTaskScheduler;
import edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduler;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ITaskScheduler;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.streaming.datalocalityaware.DataLocalityStreamingTaskScheduler;
import edu.iu.dsc.tws.tsched.streaming.firstfit.FirstFitStreamingTaskScheduler;
import edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler;

/**
 * This class invokes the appropriate task schedulers based on the 'streaming' or 'batch' task types
 * and scheduling modes 'roundrobin', 'firstfit', 'datalocality'.
 */
public class TaskScheduler implements ITaskScheduler {

  private Config config;

  private DataFlowTaskGraph dataFlowTaskGraph;

  private WorkerPlan workerPlan;

  private String schedulingType;

  @Override
  public void initialize(Config cfg) {
    this.config = cfg;
  }

  /**
   * This is the base method for the task scheduler to invoke the appropriate task schedulers
   * either "batch" or "streaming" based on the task type.
   */
  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph graph, WorkerPlan plan) {

    this.dataFlowTaskGraph = graph;
    this.workerPlan = plan;

    TaskSchedulePlan taskSchedulePlan = null;

    if (graph.getOperationMode().equals("Streaming")) {
      taskSchedulePlan = scheduleStreamingTask();
    } else if (graph.getOperationMode().equals("batch")) {
      taskSchedulePlan = scheduleBatchTask();
    }
    return taskSchedulePlan;
  }

  /**
   * This method invokes the appropriate streaming task schedulers based on the scheduling mode
   * specified in the task configuration by the user or else from the default configuration value.
   */
  public TaskSchedulePlan scheduleStreamingTask() {

    if (config.getStringValue("SchedulingMode") != null) {
      this.schedulingType = config.getStringValue("SchedulingMode");
    } else {
      this.schedulingType = TaskSchedulerContext.taskSchedulingMode(config);
    }

    TaskSchedulePlan taskSchedulePlan = null;

    if ("roundrobin".equalsIgnoreCase(schedulingType)) {

      RoundRobinTaskScheduler roundRobinTaskScheduler = new RoundRobinTaskScheduler();
      roundRobinTaskScheduler.initialize(config);
      taskSchedulePlan = roundRobinTaskScheduler.schedule(dataFlowTaskGraph, workerPlan);

    } else if ("firstfit".equalsIgnoreCase(schedulingType)) {

      FirstFitStreamingTaskScheduler firstFitStreamingTaskScheduler
              = new FirstFitStreamingTaskScheduler();
      firstFitStreamingTaskScheduler.initialize(config);
      taskSchedulePlan = firstFitStreamingTaskScheduler.schedule(dataFlowTaskGraph, workerPlan);

    } else if ("datalocalityaware".equalsIgnoreCase(schedulingType)) {

      DataLocalityStreamingTaskScheduler dataLocalityAwareTaskScheduler
              = new DataLocalityStreamingTaskScheduler();
      dataLocalityAwareTaskScheduler.initialize(config);
      taskSchedulePlan = dataLocalityAwareTaskScheduler.schedule(dataFlowTaskGraph, workerPlan);
    }
    return taskSchedulePlan;
  }

  /**
   * This method invokes the appropriate batch task schedulers based on the scheduling mode
   * specified in the task configuration by the user or else from the default configuration value.
   *
   * @return
   */
  public TaskSchedulePlan scheduleBatchTask() {

    if (config.getStringValue("SchedulingMode") != null) {
      this.schedulingType = config.getStringValue("SchedulingMode");
    } else {
      this.schedulingType = TaskSchedulerContext.taskSchedulingMode(config);
    }

    TaskSchedulePlan taskSchedulePlan = null;
    if ("roundrobin".equals(schedulingType)) {
      RoundRobinBatchTaskScheduler roundRobinBatchTaskScheduling
              = new RoundRobinBatchTaskScheduler();
      roundRobinBatchTaskScheduling.initialize(config);
      taskSchedulePlan = roundRobinBatchTaskScheduling.schedule(dataFlowTaskGraph, workerPlan);

    } else if ("datalocalityaware".equals(schedulingType)) {
      DataLocalityBatchTaskScheduler dataLocalityBatchTaskScheduling
              = new DataLocalityBatchTaskScheduler();
      dataLocalityBatchTaskScheduling.initialize(config);
      taskSchedulePlan = dataLocalityBatchTaskScheduling.schedule(dataFlowTaskGraph, workerPlan);
    }
    return taskSchedulePlan;
  }
}

