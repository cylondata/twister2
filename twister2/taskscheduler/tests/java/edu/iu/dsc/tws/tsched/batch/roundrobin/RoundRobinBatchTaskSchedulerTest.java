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
package edu.iu.dsc.tws.tsched.batch.roundrobin;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class RoundRobinBatchTaskSchedulerTest {

  @Test
  public void testUniqueSchedules() {
    int parallel = 16;
    DataFlowTaskGraph graph = createGraph(parallel);
    RoundRobinBatchTaskScheduler scheduler = new RoundRobinBatchTaskScheduler();
    scheduler.initialize(Config.newBuilder().build());
    WorkerPlan workerPlan = createWorkPlan(parallel);

    for (int i = 0; i < 10; i++) {
      TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
      TaskSchedulePlan plan2 = scheduler.schedule(graph, workerPlan);

      Assert.assertNotNull(plan1);
      Assert.assertNotNull(plan2);

      Assert.assertEquals(plan1.getContainers().size(), plan2.getContainers().size());
    }
  }

  @Test
  public void testUniqueSchedules2() {
    int parallel = 16;
    DataFlowTaskGraph graph = createGraph(parallel);
    RoundRobinBatchTaskScheduler scheduler = new RoundRobinBatchTaskScheduler();
    scheduler.initialize(Config.newBuilder().build());
    WorkerPlan workerPlan = createWorkPlan(parallel);

    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
    WorkerPlan workerPlan2 = createWorkPlan2(parallel);
    for (int i = 0; i < 10; i++) {
      TaskSchedulePlan plan2 = scheduler.schedule(graph, workerPlan2);

      Assert.assertNotNull(plan1);
      Assert.assertNotNull(plan2);

      Assert.assertEquals(plan1.getContainers().size(), plan2.getContainers().size());

      int instancescount1 = 0;

      for (TaskSchedulePlan.ContainerPlan containerPlan : plan1.getContainers()) {
        instancescount1 = containerPlan.getTaskInstances().size();
      }
      Assert.assertEquals(instancescount1, graph.getTaskVertexSet().size());
    }
  }


  private boolean containerEquals(TaskSchedulePlan.ContainerPlan p1,
                                  TaskSchedulePlan.ContainerPlan p2) {
    if (p1.getContainerId() != p2.getContainerId()) {
      return false;
    }

    if (p1.getTaskInstances().size() != p2.getTaskInstances().size()) {
      return false;
    }

    for (TaskSchedulePlan.TaskInstancePlan instancePlan : p1.getTaskInstances()) {
      if (!p2.getTaskInstances().contains(instancePlan)) {
        return false;
      }
    }
    return true;
  }

  private WorkerPlan createWorkPlan(int workers) {
    WorkerPlan plan = new WorkerPlan();
    for (int i = 0; i < workers; i++) {
      plan.addWorker(new Worker(i));
    }
    return plan;
  }

  private WorkerPlan createWorkPlan2(int workers) {
    WorkerPlan plan = new WorkerPlan();
    for (int i = workers - 1; i >= 0; i--) {
      plan.addWorker(new Worker(i));
    }
    return plan;
  }

  private DataFlowTaskGraph createGraph(int parallel) {
    TestSource ts = new TestSource();
    TestSink testSink = new TestSink();
    TestSink1 testSink1 = new TestSink1();
    TestSink2 testSink2 = new TestSink2();
    TestSink3 testSink3 = new TestSink3();

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(Config.newBuilder().build());
    builder.addSource("source", ts, 16);
    ComputeConnection c1 = builder.addSink("sink1", testSink, 16);
    ComputeConnection c2 = builder.addSink("sink2", testSink1, 16);
    ComputeConnection c3 = builder.addSink("merge", testSink2, 16);
    ComputeConnection c4 = builder.addSink("final", testSink3, 16);

    c1.partition("source", "partition-edge1", DataType.INTEGER);
    c2.partition("source", "partition-edge2", DataType.INTEGER);
    c3.partition("sink2", "partition-edge3", DataType.INTEGER);
    c4.partition("merge", "partition-edge4", DataType.INTEGER);

    builder.setMode(OperationMode.BATCH);
    return builder.build();
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

  public static class TestSink1 extends BaseSink {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public boolean execute(IMessage message) {
      return false;
    }
  }

  public static class TestSink2 extends BaseSink {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public boolean execute(IMessage message) {
      return false;
    }
  }

  public static class TestSink3 extends BaseSink {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public boolean execute(IMessage message) {
      return false;
    }
  }
}
