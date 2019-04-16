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
package edu.iu.dsc.tws.tsched.streaming.roundrobin;

import java.util.Map;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.schedule.ContainerPlan;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class RoundRobinTaskSchedulerTest {

  private static final Logger LOG = Logger.getLogger(RoundRobinTaskSchedulerTest.class.getName());
  @Test
  public void testUniqueSchedules() {
    int parallel = 256;
    DataFlowTaskGraph graph = createGraph(parallel);
    RoundRobinTaskScheduler scheduler = new RoundRobinTaskScheduler();
    scheduler.initialize(Config.newBuilder().build());

    WorkerPlan workerPlan = createWorkPlan(parallel);

    for (int i = 0; i < 1000; i++) {
      TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
      TaskSchedulePlan plan2 = scheduler.schedule(graph, workerPlan);

      Assert.assertEquals(plan1.getContainers().size(), plan2.getContainers().size());

      Map<Integer, ContainerPlan> map2 = plan2.getContainersMap();
      for (ContainerPlan containerPlan : plan1.getContainers()) {
        ContainerPlan p2 = map2.get(containerPlan.getContainerId());
        Assert.assertTrue(containerEquals(containerPlan, p2));
      }
    }
  }

  @Test
  public void testUniqueSchedules2() {
    int parallel = 256;
    DataFlowTaskGraph graph = createGraph(parallel);
    RoundRobinTaskScheduler scheduler = new RoundRobinTaskScheduler();
    scheduler.initialize(Config.newBuilder().build());

    WorkerPlan workerPlan = createWorkPlan(parallel);
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);

    WorkerPlan workerPlan2 = createWorkPlan2(parallel);
    for (int i = 0; i < 1000; i++) {
      TaskSchedulePlan plan2 = scheduler.schedule(graph, workerPlan2);

      Assert.assertEquals(plan1.getContainers().size(), plan2.getContainers().size());

      Map<Integer, ContainerPlan> map2 = plan2.getContainersMap();
      for (ContainerPlan containerPlan : plan1.getContainers()) {
        ContainerPlan p2 = map2.get(containerPlan.getContainerId());

        Assert.assertTrue(containerEquals(containerPlan, p2));
      }
    }
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

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(Config.newBuilder().build());
    builder.addSource("source", ts, parallel);
    ComputeConnection c = builder.addSink("sink", testSink, 1);
    c.reduce("source", "edge", Op.SUM, DataType.INTEGER_ARRAY);
    builder.setMode(OperationMode.STREAMING);
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
