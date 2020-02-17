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

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.Worker;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerPlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.utils.TaskSchedulerClassTest;

public class TaskSchedulerTest {

  @Test
  public void testUniqueSchedules1() {
    int parallel = 2;
    ComputeGraph graph = createStreamingGraph(parallel);
    TaskScheduler scheduler = new TaskScheduler();
    Config config = getConfig();
    scheduler.initialize(config);
    WorkerPlan workerPlan = createWorkPlan(parallel);

    if (graph.getOperationMode().equals("STREAMING")) {
      Assert.assertEquals(scheduler.getClass(),
          TaskSchedulerContext.streamingTaskSchedulingClass(config));
    }
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
    Assert.assertNotNull(plan1);
  }

  @Test
  public void testUniqueSchedules2() {
    int parallel = 2;
    ComputeGraph graph = createBatchGraph(parallel);
    TaskScheduler scheduler = new TaskScheduler();
    Config config = getConfig();
    scheduler.initialize(config);
    WorkerPlan workerPlan = createWorkPlan(parallel);

    if (graph.getOperationMode().equals("BATCH")) {
      Assert.assertEquals(scheduler.getClass(),
          TaskSchedulerContext.batchTaskSchedulingClass(config));
    }
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
    Assert.assertNotNull(plan1);
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

  private ComputeGraph createStreamingGraph(int parallel) {
    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", testSource);
    builder.setParallelism("source", parallel);

    builder.addTask("sink1", testSink);
    builder.setParallelism("sink1", parallel);

    builder.operationMode(OperationMode.STREAMING);
    return builder.build();
  }

  private ComputeGraph createBatchGraph(int parallel) {
    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", testSource);
    builder.setParallelism("source", parallel);

    builder.addTask("sink1", testSink);
    builder.setParallelism("sink1", parallel);

    builder.operationMode(OperationMode.BATCH);
    return builder.build();
  }

  private Config getConfig() {

    String twister2Home = "/home/" + System.getProperty("user.dir")
        + "/twister2/bazel-bin/scripts/package/twister2-0.5.1";
    String configDir = "/home/" + System.getProperty("user.dir")
        + "/twister2/twister2/taskscheduler/tests/conf/";
    String clusterType = "standalone";

    Config config = ConfigLoader.loadConfig(twister2Home, configDir, clusterType);
    return Config.newBuilder().putAll(config).build();
  }
}
