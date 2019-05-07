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

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.schedule.ContainerPlan;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class TaskSchedulerTest {

  @Test
  public void testUniqueSchedules1() {
    int parallel = 2;
    DataFlowTaskGraph graph = createStreamingGraph(parallel);
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
    DataFlowTaskGraph graph = createBatchGraph(parallel);
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

  private DataFlowTaskGraph createStreamingGraph(int parallel) {
      TestSource ts = new TestSource();
      TestSink testSink = new TestSink();

      GraphBuilder builder = GraphBuilder.newBuilder();
      builder.addSource("source", ts);
      builder.setParallelism("source", parallel);

      builder.addSink("sink1", testSink);
      builder.setParallelism("sink1", parallel);

      builder.operationMode(OperationMode.STREAMING);
      return builder.build();
  }

  private DataFlowTaskGraph createBatchGraph(int parallel) {
    TestSource ts = new TestSource();
    TestSink testSink = new TestSink();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", ts);
    builder.setParallelism("source", parallel);

    builder.addSink("sink1", testSink);
    builder.setParallelism("sink1", parallel);

    builder.operationMode(OperationMode.BATCH);
    return builder.build();
  }

  private Config getConfig() {

    String twister2Home = "/home/" + System.getProperty("user.dir")
        + "/twister2/bazel-bin/scripts/package/twister2-0.2.1";
    String configDir = "/home/" + System.getProperty("user.dir")
    + "/twister2/twister2/taskscheduler/tests/conf/";
    String clusterType = "standalone";

    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);
    return Config.newBuilder().putAll(config).build();
  }

  public static class TestSource extends BaseSource {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public void execute() {
    }
  }

  public static class TestSink extends BaseSink {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public boolean execute(IMessage message) {
      return false;
    }
  }
}
