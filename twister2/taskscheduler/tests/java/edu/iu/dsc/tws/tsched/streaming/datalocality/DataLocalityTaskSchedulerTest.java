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

import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskInstancePlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.Worker;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerPlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerSchedulePlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.streaming.datalocalityaware.DataLocalityStreamingTaskScheduler;
import edu.iu.dsc.tws.tsched.utils.DataGenerator;
import edu.iu.dsc.tws.tsched.utils.TaskSchedulerClassTest;

public class DataLocalityTaskSchedulerTest {

  private static final Logger LOG = Logger.getLogger(DataLocalityTaskSchedulerTest.class.getName());

  @Test
  public void testUniqueSchedules1() {
    int parallel = 2;
    int workers = 2;
    ComputeGraph graph = createGraph(parallel);
    DataLocalityStreamingTaskScheduler scheduler = new DataLocalityStreamingTaskScheduler();
    Config config = getConfig();
    scheduler.initialize(config, 1);
    generateData(config);
    WorkerPlan workerPlan = createWorkPlan(workers);
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);

    WorkerPlan workerPlan2 = createWorkPlan2(workers);
    for (int i = 0; i < 10; i++) {
      TaskSchedulePlan plan2 = scheduler.schedule(graph, workerPlan2);
      Assert.assertEquals(plan1.getContainers().size(), plan2.getContainers().size());
      Map<Integer, WorkerSchedulePlan> containersMap = plan2.getContainersMap();
      for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
        WorkerSchedulePlan workerSchedulePlan = entry.getValue();
        Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();
        Assert.assertEquals(containerPlanTaskInstances.size(),
            TaskSchedulerContext.defaultTaskInstancesPerContainer(config));
      }
    }
  }

  @Test
  public void testUniqueSchedules2() {
    int parallel = 10;
    int workers = 2;

    ComputeGraph graph = createGraphWithConstraints(parallel);
    DataLocalityStreamingTaskScheduler scheduler = new DataLocalityStreamingTaskScheduler();
    Config config = getConfig();

    scheduler.initialize(config, 1);
    generateData(config);
    WorkerPlan workerPlan = createWorkPlan(workers);
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
    Assert.assertNotNull(plan1);

    Map<Integer, WorkerSchedulePlan> containersMap = plan1.getContainersMap();
    for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
      WorkerSchedulePlan workerSchedulePlan = entry.getValue();
      Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();

      Assert.assertEquals(containerPlanTaskInstances.size(),
          Integer.parseInt(graph.getGraphConstraints().get(
              Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER)));
    }
  }

  @Test
  public void testUniqueSchedules3() {
    int parallel = 10;
    int workers = 3;

    ComputeGraph graph = createGraphWithComputeTaskAndConstraints(parallel);
    DataLocalityStreamingTaskScheduler scheduler = new DataLocalityStreamingTaskScheduler();
    Config config = getConfig();

    scheduler.initialize(config, 1);
    generateData(config);
    WorkerPlan workerPlan = createWorkPlan(workers);
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
    Assert.assertNotNull(plan1);

    Map<Integer, WorkerSchedulePlan> containersMap = plan1.getContainersMap();
    for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {

      WorkerSchedulePlan workerSchedulePlan = entry.getValue();
      Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();

      Assert.assertEquals(containerPlanTaskInstances.size(),
          Integer.parseInt(graph.getGraphConstraints().get(
              Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER)));
    }
  }

  private Config getConfig() {
    Config config = ConfigLoader.loadTestConfig();
    return Config.newBuilder()
        .put(DataObjectConstants.DINPUT_DIRECTORY, "/tmp/dinput")
        .put(DataObjectConstants.FILE_SYSTEM, "local")
        .put(DataObjectConstants.DSIZE, "1000")
        .put(DataObjectConstants.DIMENSIONS, "2")
        .putAll(config).build();
  }

  private void generateData(Config config) {
    DataGenerator dataGenerator = new DataGenerator(config);
    dataGenerator.generate(
        new Path(String.valueOf(config.get(DataObjectConstants.DINPUT_DIRECTORY))),
        Integer.parseInt(String.valueOf(config.get(DataObjectConstants.DSIZE))),
        Integer.parseInt(String.valueOf(config.get(DataObjectConstants.DIMENSIONS))));
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

  private ComputeGraph createGraph(int parallel) {

    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    ComputeGraphBuilder computeGraphBuilder =
        ComputeGraphBuilder.newBuilder(Config.newBuilder().build());
    computeGraphBuilder.addSource("source", testSource, parallel);
    ComputeConnection computeConnection = computeGraphBuilder.addCompute("sink", testSink,
        parallel);
    computeConnection.direct("source")
        .viaEdge("direct-edge")
        .withDataType(MessageTypes.OBJECT);
    computeGraphBuilder.setMode(OperationMode.STREAMING);

    ComputeGraph taskGraph = computeGraphBuilder.build();
    return taskGraph;
  }

  private ComputeGraph createGraphWithConstraints(int parallel) {
    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    ComputeGraphBuilder computeGraphBuilder =
        ComputeGraphBuilder.newBuilder(Config.newBuilder().build());
    computeGraphBuilder.addSource("source", testSource, parallel);
    ComputeConnection computeConnection = computeGraphBuilder.addCompute("sink", testSink,
        parallel);
    computeConnection.direct("source")
        .viaEdge("direct-edge")
        .withDataType(MessageTypes.OBJECT);
    computeGraphBuilder.setMode(OperationMode.STREAMING);
    computeGraphBuilder.addGraphConstraints(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "10");
    ComputeGraph taskGraph = computeGraphBuilder.build();
    return taskGraph;
  }


  private ComputeGraph createGraphWithComputeTaskAndConstraints(int parallel) {

    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestCompute testCompute = new TaskSchedulerClassTest.TestCompute();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    ComputeGraphBuilder computeGraphBuilder =
        ComputeGraphBuilder.newBuilder(Config.newBuilder().build());
    computeGraphBuilder.addSource("source", testSource, parallel);

    ComputeConnection computeConnection = computeGraphBuilder.addCompute(
        "compute", testCompute, parallel);
    ComputeConnection sinkComputeConnection = computeGraphBuilder.addCompute(
        "sink", testSink, parallel);

    computeConnection.direct("source")
        .viaEdge("cdirect-edge")
        .withDataType(MessageTypes.OBJECT);

    sinkComputeConnection.direct("compute")
        .viaEdge("sdirect-edge")
        .withDataType(MessageTypes.OBJECT);

    computeGraphBuilder.setMode(OperationMode.STREAMING);

    computeGraphBuilder.addGraphConstraints(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "10");
    ComputeGraph taskGraph = computeGraphBuilder.build();
    return taskGraph;
  }
}
