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
package edu.iu.dsc.tws.examples.batch.kmeans;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskExecutor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.BaseCompute;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.taskscheduler.TaskScheduler;

public class MultiComputeTaskGraphTest {

  private static final Logger LOG = Logger.getLogger(MultiComputeTaskGraphTest.class.getName());

  @Test
  public void testUniqueSchedules1() {
    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(getConfig());
    int parallel = 2;
    SourceTask sourceTask = new SourceTask();
    FirstComputeTask firstComputeTask = new FirstComputeTask();
    SecondComputeTask secondComputeTask = new SecondComputeTask();
    ReduceTask reduceTask = new ReduceTask();

    builder.addSource("source", sourceTask, parallel);
    ComputeConnection firstComputeConnection = builder.addCompute(
        "firstcompute", firstComputeTask, parallel);
    ComputeConnection secondComputeConnection = builder.addCompute(
        "secondcompute", secondComputeTask, parallel);
    ComputeConnection reduceConnection = builder.addSink("sink", reduceTask, parallel);

    firstComputeConnection.direct("source", "fdirect", DataType.OBJECT);
    secondComputeConnection.direct("source", "sdirect", DataType.OBJECT);
    reduceConnection.allreduce("firstcompute", "f-reduce", new Aggregator(), DataType.OBJECT);
    reduceConnection.allreduce("secondcompute", "s-reduce", new Aggregator(), DataType.OBJECT);

    builder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph graph = builder.build();
    LOG.info("Graph Values:" + graph.getTaskVertexSet());
    Assert.assertNotNull(graph);

    TaskScheduler taskScheduler = new TaskScheduler();
    taskScheduler.initialize(getConfig());
    TaskSchedulePlan taskSchedulePlan = taskScheduler.schedule(graph, createWorkPlan(2));
    Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap
        = taskSchedulePlan.getContainersMap();
    for (Map.Entry<Integer, TaskSchedulePlan.ContainerPlan> entry : containersMap.entrySet()) {
      Integer integer = entry.getKey();
      TaskSchedulePlan.ContainerPlan containerPlan = entry.getValue();
      Set<TaskSchedulePlan.TaskInstancePlan> taskInstancePlans
          = containerPlan.getTaskInstances();
      LOG.info("Task Details for Container Id:" + integer);
      for (TaskSchedulePlan.TaskInstancePlan ip : taskInstancePlans) {
        LOG.info("Task Id:" + ip.getTaskId() + "\tTask Index" + ip.getTaskIndex()
            + "\tTask Name:" + ip.getTaskName());
      }
    }
    TaskExecutor taskExecutor = null;
    ExecutionPlan plan = taskExecutor.plan(graph);
    Assert.assertNotNull(plan);
  }

  private static class SourceTask extends BaseSource {
    private static final long serialVersionUID = -254264120110286748L;

    private double[] datapoints = null;
    private int numPoints = 100;

    @Override
    public void execute() {
      int seedValue = 100;
      datapoints = new double[numPoints];
      Random r = new Random(seedValue);
      for (int i = 0; i < numPoints; i++) {
        double randomValue = r.nextDouble();
        datapoints[i] = randomValue;
      }
      context.write("fdirect", datapoints);
      context.writeEnd("sdirect", datapoints);
    }

    public void prepare(Config cfg, TaskContext context) {
      super.prepare(cfg, context);
    }
  }

  private static class FirstComputeTask extends BaseCompute {
    private static final long serialVersionUID = -254264120110286748L;

    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received Points: " + context.getWorkerId()
          + ":" + context.taskId());
      if (message.getContent() instanceof Iterator) {
        Iterator it = (Iterator) message.getContent();
        while (it.hasNext()) {
          count += 1;
          context.write("f-reduce", it.next());
        }
      }
      LOG.info(String.format("%d %d All-Reduce Received count: %d", context.getWorkerId(),
          context.taskId(), count));
      context.end("f-reduce");
      return true;
    }
  }

  private static class SecondComputeTask extends BaseCompute {
    private static final long serialVersionUID = -254264120110286748L;

    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received Points: " + context.getWorkerId()
          + ":" + context.taskId());
      if (message.getContent() instanceof Iterator) {
        Iterator it = (Iterator) message.getContent();
        while (it.hasNext()) {
          count += 1;
          context.write("s-reduce", it.next());
        }
      }
      LOG.info(String.format("%d %d All-Reduce Received count: %d", context.getWorkerId(),
          context.taskId(), count));
      context.end("s-reduce");
      return true;
    }
  }

  private static class ReduceTask extends BaseSink {
    private static final long serialVersionUID = -5190777711234234L;

    private double[] datapoints;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received Points: " + context.getWorkerId()
          + ":" + context.taskId());
      datapoints = (double[]) message.getContent();
      return true;
    }
  }

  public class Aggregator implements IFunction {
    private static final long serialVersionUID = -254264120110286748L;

    @Override
    public Object onMessage(Object object1, Object object2) throws ArrayIndexOutOfBoundsException {

      double[] object11 = (double[]) object1;
      double[] object21 = (double[]) object2;

      double[] object31 = new double[object11.length];

      for (int j = 0; j < object11.length; j++) {
        double newVal = object11[j] + object21[j];
        object31[j] = newVal;
      }
      return object31;
    }
  }

  private Config getConfig() {
    String twister2Home = "/home/kannan/twister2/bazel-bin/scripts/package/twister2-0.1.0";
    String configDir = "/home/kannan/twister2/twister2/taskscheduler/tests/conf/";
    String clusterType = "standalone";
    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);
    return Config.newBuilder().putAll(config).build();
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
}
