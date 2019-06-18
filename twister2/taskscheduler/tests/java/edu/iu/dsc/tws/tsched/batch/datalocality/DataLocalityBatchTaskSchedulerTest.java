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
package edu.iu.dsc.tws.tsched.batch.datalocality;

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
import edu.iu.dsc.tws.comms.api.MessageTypes;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.task.api.schedule.ContainerPlan;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.batch.datalocalityaware.DataLocalityBatchTaskScheduler;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.TaskSchedulerClassTest;

public class DataLocalityBatchTaskSchedulerTest {

  private static final Logger LOG = Logger.getLogger(
      DataLocalityBatchTaskSchedulerTest.class.getName());

  @Test
  public void testUniqueSchedules1() {
    int parallel = 4;
    int workers = 2;
    DataFlowTaskGraph graph = createGraph(parallel);
    DataLocalityBatchTaskScheduler scheduler = new DataLocalityBatchTaskScheduler();
    Config config = getConfig();
    scheduler.initialize(config, 1);

    WorkerPlan workerPlan = createWorkPlan(workers);
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);

    WorkerPlan workerPlan2 = createWorkPlan2(workers);
    for (int i = 0; i < 100; i++) {
      TaskSchedulePlan plan2 = scheduler.schedule(graph, workerPlan2);

      Assert.assertEquals(plan1.getContainers().size(), plan2.getContainers().size());

      Map<Integer, ContainerPlan> containersMap = plan2.getContainersMap();
      for (Map.Entry<Integer, ContainerPlan> entry : containersMap.entrySet()) {
        ContainerPlan containerPlan = entry.getValue();
        Set<TaskInstancePlan> containerPlanTaskInstances = containerPlan.getTaskInstances();
        Assert.assertEquals(containerPlanTaskInstances.size() / graph.getTaskVertexSet().size(),
            TaskSchedulerContext.defaultTaskInstancesPerContainer(config));
      }
    }
  }

  @Test
  public void testUniqueSchedules2() {
    int parallel = 4;
    int workers = 2;

    DataFlowTaskGraph graph = createGraphWithConstraints(parallel);
    DataLocalityBatchTaskScheduler scheduler = new DataLocalityBatchTaskScheduler();
    Config config = getConfig();

    scheduler.initialize(config, 1);
    WorkerPlan workerPlan = createWorkPlan(workers);
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
    Assert.assertNotNull(plan1);

    Map<Integer, ContainerPlan> containersMap = plan1.getContainersMap();
    for (Map.Entry<Integer, ContainerPlan> entry : containersMap.entrySet()) {
      ContainerPlan containerPlan = entry.getValue();
      Set<TaskInstancePlan> containerPlanTaskInstances = containerPlan.getTaskInstances();
      Assert.assertEquals(containerPlanTaskInstances.size(), parallel);
    }
  }

  @Test
  public void testUniqueSchedules3() {
    int parallel = 4;
    int workers = 2;

    DataFlowTaskGraph graph = createGraphWithComputeTaskAndConstraints(parallel);
    DataLocalityBatchTaskScheduler scheduler = new DataLocalityBatchTaskScheduler();
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
          workers * graph.getTaskVertexSet().size());
    }
  }

  @Test
  public void testUniqueSchedules4() {
    int parallel = 4;
    int workers = 2;

    DataFlowTaskGraph graph = createGraphWithMultipleComputeTaskAndConstraints(parallel);
    DataLocalityBatchTaskScheduler scheduler = new DataLocalityBatchTaskScheduler();
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
          workers * graph.getTaskVertexSet().size());
    }
  }

  private Config getConfig() {
    String twister2Home = "/home/" + System.getProperty("user.dir")
        + "/twister2/bazel-bin/scripts/package/twister2-0.2.2";
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

  private DataFlowTaskGraph createGraph(int parallel) {

    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(Config.newBuilder().build());
    builder.addSource("source", testSource, parallel);
    ComputeConnection sinkConnection = builder.addSink("sink", testSink, parallel);

    sinkConnection.direct("source")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    builder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph graph = builder.build();
    return graph;
  }

  private DataFlowTaskGraph createGraphWithConstraints(int parallel) {
    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    TaskGraphBuilder taskGraphBuilder = TaskGraphBuilder.newBuilder(Config.newBuilder().build());
    taskGraphBuilder.addSource("source", testSource, parallel);
    ComputeConnection computeConnection = taskGraphBuilder.addSink("sink", testSink,
        parallel);
    computeConnection.direct("source")
        .viaEdge("direct-edge")
        .withDataType(MessageTypes.OBJECT);
    taskGraphBuilder.setMode(OperationMode.STREAMING);

    taskGraphBuilder.addGraphConstraints(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "2");
    DataFlowTaskGraph taskGraph = taskGraphBuilder.build();
    return taskGraph;
  }

  private DataFlowTaskGraph createGraphWithComputeTaskAndConstraints(int parallel) {

    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestCompute testCompute = new TaskSchedulerClassTest.TestCompute();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(Config.newBuilder().build());
    builder.addSource("source", testSource, parallel);
    ComputeConnection computeConnection = builder.addCompute("compute", testCompute, parallel);
    ComputeConnection sinkConnection = builder.addSink("sink", testSink, parallel);

    computeConnection.direct("source")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);

    sinkConnection.direct("compute")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    builder.setMode(OperationMode.BATCH);

    builder.addGraphConstraints(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "2");
    DataFlowTaskGraph graph = builder.build();
    return graph;
  }


  private DataFlowTaskGraph createGraphWithMultipleComputeTaskAndConstraints(int parallel) {

    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestCompute firstTestCompute = new TaskSchedulerClassTest.TestCompute();
    TaskSchedulerClassTest.TestComputeChild secondTestCompute
        = new TaskSchedulerClassTest.TestComputeChild();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(Config.newBuilder().build());
    builder.addSource("source", testSource, parallel);
    ComputeConnection firstComputeConnection
        = builder.addCompute("firstcompute", firstTestCompute, parallel);
    ComputeConnection secondComputeConnection
        = builder.addCompute("secondcompute", secondTestCompute, parallel);
    ComputeConnection sinkConnection = builder.addSink("sink", testSink, parallel);

    firstComputeConnection.direct("source")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);

    secondComputeConnection.direct("source")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);

    sinkConnection.allreduce("firstcompute")
        .viaEdge("freduce")
        .withReductionFunction(new TaskSchedulerClassTest.Aggregator())
        .withDataType(MessageTypes.OBJECT);

    sinkConnection.allreduce("secondcompute")
        .viaEdge("sreduce")
        .withReductionFunction(new TaskSchedulerClassTest.Aggregator())
        .withDataType(MessageTypes.OBJECT);

    builder.setMode(OperationMode.BATCH);

    builder.addGraphConstraints(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "2");
    DataFlowTaskGraph graph = builder.build();
    return graph;
  }
}
