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
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class TaskSchedulerTest {
  @Test
  public void testUniqueSchedules() {
    int parallel = 2;
    DataFlowTaskGraph graph = createGraph(parallel);
    TaskScheduler scheduler = new TaskScheduler();
    scheduler.initialize(Config.newBuilder().build());

    WorkerPlan workerPlan = createWorkPlan(parallel);

    for (int i = 0; i < 1000; i++) {
      TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
      TaskSchedulePlan plan2 = scheduler.schedule(graph, workerPlan);

      Assert.assertEquals(plan1.getContainers().size(), plan2.getContainers().size());

      Map<Integer, TaskSchedulePlan.ContainerPlan> map2 = plan2.getContainersMap();
      for (TaskSchedulePlan.ContainerPlan containerPlan : plan1.getContainers()) {
        TaskSchedulePlan.ContainerPlan p2 = map2.get(containerPlan.getContainerId());

        Assert.assertTrue(containerEquals(containerPlan, p2));
      }
    }
  }

  @Test
  public void testUniqueSchedules2() {
    int parallel = 256;
    DataFlowTaskGraph graph = createGraph(parallel);
    TaskScheduler scheduler = new TaskScheduler();
    scheduler.initialize(Config.newBuilder().build());

    WorkerPlan workerPlan = createWorkPlan(parallel);
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);

    WorkerPlan workerPlan2 = createWorkPlan2(parallel);
    for (int i = 0; i < 1000; i++) {
      TaskSchedulePlan plan2 = scheduler.schedule(graph, workerPlan2);

      Assert.assertEquals(plan1.getContainers().size(), plan2.getContainers().size());

      Map<Integer, TaskSchedulePlan.ContainerPlan> map2 = plan2.getContainersMap();
      for (TaskSchedulePlan.ContainerPlan containerPlan : plan1.getContainers()) {
        TaskSchedulePlan.ContainerPlan p2 = map2.get(containerPlan.getContainerId());

        Assert.assertTrue(containerEquals(containerPlan, p2));
      }
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
      plan.addWorker(new Worker(i));
    }
    return plan;
  }

  private DataFlowTaskGraph createGraph(int parallel) {
    TestSource ts = new TestSource();
    TestSink testSink = new TestSink();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", ts);
    builder.setParallelism("source", 2);

    builder.addSink("sink1", testSink);
    builder.setParallelism("sink1", 2);

    builder.operationMode(OperationMode.BATCH);
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
}
