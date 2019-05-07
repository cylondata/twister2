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
package edu.iu.dsc.tws.tsched.streaming.datalocality;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.schedule.ContainerPlan;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.streaming.datalocalityaware.DataLocalityStreamingTaskScheduler;

public class DataLocalityTaskSchedulerTest {

  private static final Logger LOG = Logger.getLogger(DataLocalityTaskSchedulerTest.class.getName());

  @Test
  public void testUniqueSchedules1() {
    int parallel = 1000;
    int workers = 2;
    DataFlowTaskGraph graph = createGraph(parallel);
    DataLocalityStreamingTaskScheduler scheduler = new DataLocalityStreamingTaskScheduler();
    Config config = getConfig();
    scheduler.initialize(config, 1);

    WorkerPlan workerPlan = createWorkPlan(workers);
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);

    WorkerPlan workerPlan2 = createWorkPlan2(workers);
    for (int i = 0; i < 1000; i++) {
      TaskSchedulePlan plan2 = scheduler.schedule(graph, workerPlan2);

      Assert.assertEquals(plan1.getContainers().size(), plan2.getContainers().size());

      Map<Integer, ContainerPlan> containersMap = plan2.getContainersMap();
      for (Map.Entry<Integer, ContainerPlan> entry : containersMap.entrySet()) {
        ContainerPlan containerPlan = entry.getValue();
        Set<TaskInstancePlan> containerPlanTaskInstances = containerPlan.getTaskInstances();
        Assert.assertEquals(containerPlanTaskInstances.size(),
            TaskSchedulerContext.defaultTaskInstancesPerContainer(config));
      }
    }
  }

  @Test
  public void testUniqueSchedules2() {
    int parallel = 1000;
    int workers = 2;

    DataFlowTaskGraph graph = createGraphWithConstraints(parallel);
    DataLocalityStreamingTaskScheduler scheduler = new DataLocalityStreamingTaskScheduler();
    Config config = getConfig();

    scheduler.initialize(config, 1);
    WorkerPlan workerPlan = createWorkPlan(workers);
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
    Assert.assertNotNull(plan1);


    Map<Integer, ContainerPlan> containersMap = plan1.getContainersMap();
    for (Map.Entry<Integer, ContainerPlan> entry : containersMap.entrySet()) {

      ContainerPlan containerPlan = entry.getValue();
      Set<TaskInstancePlan> containerPlanTaskInstances = containerPlan.getTaskInstances();

      Assert.assertEquals(containerPlanTaskInstances.size(),
          Integer.parseInt(graph.getGraphConstraints().get(
              Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER)));
    }
  }

  private Config getConfig() {
    String twister2Home = "/home/" + System.getProperty("user.dir")
        + "/twister2/bazel-bin/scripts/package/twister2-0.2.1";
    String configDir = "/home/" + System.getProperty("user.dir")
        + "/twister2/twister2/taskscheduler/tests/conf/";
    String clusterType = "standalone";

    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);

    JobConfig jobConfig = new JobConfig();
    jobConfig.put(DataObjectConstants.DINPUT_DIRECTORY, "/tmp/dinput");
    jobConfig.put(DataObjectConstants.FILE_SYSTEM, "local");
    return Config.newBuilder().putAll(config).putAll(jobConfig).build();
  }

  private WorkerPlan createWorkPlan(int workers) {
    WorkerPlan plan = new WorkerPlan();
    for (int i = 0; i < workers; i++) {
      Worker w = new Worker(i);
      w.addProperty("bandwidth", 1000.0);
      w.addProperty("latency", 0.1);
      plan.addWorker(w);
    }
    return plan;
  }

  private WorkerPlan createWorkPlan2(int workers) {
    WorkerPlan plan = new WorkerPlan();
    for (int i = workers - 1; i >= 0; i--) {
      Worker w = new Worker(i);
      w.addProperty("bandwidth", 1000.0);
      w.addProperty("latency", 0.1);
      plan.addWorker(w);
    }
    return plan;
  }

  private boolean containerEquals(ContainerPlan p1,
                                  ContainerPlan p2) {
    if (p1.getContainerId() != p2.getContainerId()) {
      return false;
    }

    if (p1.getTaskInstances().size() != p2.getTaskInstances().size()) {
      return false;
    }

    for (TaskInstancePlan instancePlan : p1.getTaskInstances()) {
      if (!p2.getTaskInstances().contains(instancePlan)) {
        return false;
      }
    }
    return true;
  }

  private DataFlowTaskGraph createGraphWithConstraints(int parallel) {
    GeneratorTask dataObjectSource = new GeneratorTask();
    ReceivingTask dataObjectSink = new ReceivingTask();

    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(Config.newBuilder().build());
    taskGraphBuilder.addSource("datapointsource", dataObjectSource, parallel);
    ComputeConnection computeConnection = taskGraphBuilder.addSink("datapointsink", dataObjectSink,
        parallel);
    computeConnection.partition("datapointsource", "partition-edge", DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.STREAMING);

    //Adding the user-defined constraints to the graph
    Map<String, String> sourceTaskConstraintsMap = new HashMap<>();
    sourceTaskConstraintsMap.put(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "1000");
    //sourceTaskConstraintsMap.put(Context.TWISTER2_TASK_INSTANCE_ODD_PARALLELISM, "1");

    Map<String, String> sinkTaskConstraintsMap = new HashMap<>();
    sinkTaskConstraintsMap.put(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "1000");
    //sinkTaskConstraintsMap.put(Context.TWISTER2_TASK_INSTANCE_ODD_PARALLELISM, "1");

    //Creating the communication edges between the tasks for the second task graph
    //taskGraphBuilder.addNodeConstraints("datapointsource", sourceTaskConstraintsMap);
    //taskGraphBuilder.addNodeConstraints("datapointsink", sinkTaskConstraintsMap);
    taskGraphBuilder.addGraphConstraints(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "1000");
    DataFlowTaskGraph taskGraph = taskGraphBuilder.build();
    return taskGraph;
  }


  private DataFlowTaskGraph createGraph(int parallel) {

    GeneratorTask dataObjectSource = new GeneratorTask();
    ReceivingTask dataObjectSink = new ReceivingTask();

    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(Config.newBuilder().build());
    taskGraphBuilder.addSource("datapointsource", dataObjectSource, parallel);
    ComputeConnection computeConnection = taskGraphBuilder.addSink("datapointsink", dataObjectSink,
        parallel);
    computeConnection.partition("datapointsource", "partition-edge", DataType.OBJECT);
    taskGraphBuilder.setMode(OperationMode.STREAMING);

    DataFlowTaskGraph taskGraph = taskGraphBuilder.build();
    return taskGraph;
  }

  private static class GeneratorTask extends BaseSource {

    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      this.context = ctx;
      this.config = cfg;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
      boolean wrote = context.write("partition-edge", "Hello");
      if (wrote) {
        count++;
        if (count % 100 == 0) {
          LOG.info(String.format("%d %d Message sent count : %d", context.getWorkerId(),
              context.globalTaskId(), count));
        }
      }
    }
  }

  private static class ReceivingTask extends BaseSink {

    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      this.context = ctx;
      this.config = cfg;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof String) {
        count += ((String) message.getContent()).length();
      }
      LOG.info(String.format("%d %d Message Received count: %d", context.getWorkerId(),
          context.globalTaskId(), count));
      return true;
    }
  }
}
