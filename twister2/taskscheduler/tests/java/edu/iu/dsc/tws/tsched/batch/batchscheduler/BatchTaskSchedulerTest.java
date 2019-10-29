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
package edu.iu.dsc.tws.tsched.batch.batchscheduler;

import java.util.Arrays;
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
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.tsched.utils.TaskSchedulerClassTest;

public class BatchTaskSchedulerTest {

  private static final Logger LOG = Logger.getLogger(BatchTaskSchedulerTest.class.getName());

  @Test
  public void testUniqueSchedules1() {

    int parallel = 16;
    int workers = 2;
    ComputeGraph graph = createGraph(parallel, "graph");
    BatchTaskScheduler scheduler = new BatchTaskScheduler();
    scheduler.initialize(Config.newBuilder().build());
    WorkerPlan workerPlan = createWorkPlan(workers);

    for (int i = 0; i < 1; i++) {
      TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
      TaskSchedulePlan plan2 = scheduler.schedule(graph, workerPlan);
      Assert.assertEquals(plan1.getContainers().size(), plan2.getContainers().size());

      Map<Integer, WorkerSchedulePlan> containersMap = plan1.getContainersMap();
      for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
        WorkerSchedulePlan workerSchedulePlan = entry.getValue();
        Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();
        Assert.assertEquals(containerPlanTaskInstances.size(),
            graph.vertex("source").getParallelism());
      }
    }
  }

  @Test
  public void testUniqueSchedules2() {

    int parallel = 16;
    int workers = 2;
    ComputeGraph graph = createGraph(parallel, "graph");
    BatchTaskScheduler scheduler = new BatchTaskScheduler();
    scheduler.initialize(Config.newBuilder().build());
    WorkerPlan workerPlan = createWorkPlan(workers);

    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);
    WorkerPlan workerPlan2 = createWorkPlan2(workers);
    for (int i = 0; i < 1000; i++) {
      TaskSchedulePlan plan2 = scheduler.schedule(graph, workerPlan2);
      Assert.assertEquals(plan1.getContainers().size(), plan2.getContainers().size());
    }
  }

  @Test
  public void testUniqueSchedules3() {
    int parallel = 16;
    int workers = 2;

    ComputeGraph graph = createGraphWithComputeTaskAndConstraints(parallel, "graph");
    BatchTaskScheduler scheduler = new BatchTaskScheduler();
    scheduler.initialize(Config.newBuilder().build());

    WorkerPlan workerPlan = createWorkPlan(workers);
    TaskSchedulePlan plan1 = scheduler.schedule(graph, workerPlan);

    Map<Integer, WorkerSchedulePlan> containersMap = plan1.getContainersMap();
    for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
      WorkerSchedulePlan workerSchedulePlan = entry.getValue();
      Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();
      Assert.assertEquals(containerPlanTaskInstances.size() / graph.getTaskVertexSet().size(),
          Integer.parseInt(graph.getGraphConstraints().get(
              Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER)));
    }
  }

  @Test
  public void testUniqueSchedules4() {
    int parallel = 400;
    int workers = 200;

    ComputeGraph[] graph = new ComputeGraph[3];
    Arrays.setAll(graph, i -> createGraph(parallel, "graph" + i));

    BatchTaskScheduler scheduler = new BatchTaskScheduler();
    scheduler.initialize(Config.newBuilder().build());
    WorkerPlan workerPlan = createWorkPlan(workers);

    Map<String, TaskSchedulePlan> taskSchedulePlanMap
        = scheduler.schedule(workerPlan, graph[0], graph[1], graph[2]);

    for (Map.Entry<String, TaskSchedulePlan> taskSchedulePlanEntry
        : taskSchedulePlanMap.entrySet()) {
      TaskSchedulePlan taskSchedulePlan = taskSchedulePlanEntry.getValue();
      Map<Integer, WorkerSchedulePlan> containersMap = taskSchedulePlan.getContainersMap();
      for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
        WorkerSchedulePlan workerSchedulePlan = entry.getValue();
        Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();
        Assert.assertEquals(containerPlanTaskInstances.size(),
            (parallel / workers) * graph[0].getTaskVertexSet().size());
      }
    }
  }

  @Test
  public void testUniqueSchedules5() {

    int parallel = 200;
    int workers = 400;

    ComputeGraph[] graph = new ComputeGraph[3];
    Arrays.setAll(graph, i -> createGraph(parallel, "graph" + i));

    BatchTaskScheduler scheduler = new BatchTaskScheduler();
    scheduler.initialize(Config.newBuilder().build());
    WorkerPlan workerPlan = createWorkPlan(workers);

    Map<String, TaskSchedulePlan> plan1 = scheduler.schedule(workerPlan, graph[0], graph[1]);

    for (Map.Entry<String, TaskSchedulePlan> taskSchedulePlanEntry : plan1.entrySet()) {
      TaskSchedulePlan plan2 = taskSchedulePlanEntry.getValue();
      Map<Integer, WorkerSchedulePlan> containersMap = plan2.getContainersMap();
      int index = 0;
      for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
        WorkerSchedulePlan workerSchedulePlan = entry.getValue();
        Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();
        index++;
        if (index <= parallel) {
          Assert.assertEquals(containerPlanTaskInstances.size(), workers / parallel);
        } else {
          Assert.assertEquals(containerPlanTaskInstances.size(), 0);
        }
      }
    }
  }

  @Test
  public void testUniqueSchedules6() {

    int parallel = 200;
    int workers = 400;

    ComputeGraph[] graph = new ComputeGraph[3];
    Arrays.setAll(graph, i -> createGraphWithDifferentParallelism(parallel, "graph" + i));

    BatchTaskScheduler scheduler = new BatchTaskScheduler();
    scheduler.initialize(Config.newBuilder().build());
    WorkerPlan workerPlan = createWorkPlan(workers);

    Map<String, TaskSchedulePlan> plan1
        = scheduler.schedule(workerPlan, graph[0], graph[1]);

    for (Map.Entry<String, TaskSchedulePlan> taskSchedulePlanEntry : plan1.entrySet()) {
      TaskSchedulePlan plan2 = taskSchedulePlanEntry.getValue();
      Map<Integer, WorkerSchedulePlan> containersMap = plan2.getContainersMap();
      int index = 0;
      for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
        WorkerSchedulePlan workerSchedulePlan = entry.getValue();
        Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();
        index++;
        if (index <= parallel) {
          Assert.assertEquals(containerPlanTaskInstances.size(), workers / parallel);
        } else {
          Assert.assertEquals(containerPlanTaskInstances.size(), 0);
        }
      }
    }
  }

  @Test
  public void testUniqueSchedules7() {
    int parallel = 2;
    int workers = 4;

    ComputeGraph[] graph = new ComputeGraph[3];
    graph[0] = createGraphWithDifferentParallelismInput(parallel, "graph" + 0, "a");
    graph[1] = createGraphWithDifferentParallelismInput(parallel, "graph" + 1, "b");

    //if you specify any parallel value other than 2, it will throw
    // edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException: Please verify the dependent
    // collector(s) and receptor(s) parallelism values which are not equal
    graph[2] = createGraphWithDifferentParallelismInput(parallel, "graph" + 2, "b");

    BatchTaskScheduler scheduler = new BatchTaskScheduler();
    scheduler.initialize(Config.newBuilder().build());
    WorkerPlan workerPlan = createWorkPlan(workers);

    Map<String, TaskSchedulePlan> plan1 = scheduler.schedule(workerPlan, graph);

    for (Map.Entry<String, TaskSchedulePlan> taskSchedulePlanEntry : plan1.entrySet()) {
      TaskSchedulePlan plan2 = taskSchedulePlanEntry.getValue();
      Map<Integer, WorkerSchedulePlan> containersMap = plan2.getContainersMap();
      int index = 0;
      for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
        WorkerSchedulePlan workerSchedulePlan = entry.getValue();
        Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();
        index++;
        if (index <= parallel) {
          Assert.assertEquals(containerPlanTaskInstances.size(), workers / parallel);
        } else {
          Assert.assertEquals(containerPlanTaskInstances.size(), 0);
        }
      }
    }
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

  private ComputeGraph createGraphWithDifferentParallelismInput(int parallel, String graphName,
                                                                String... inputKey) {

    TaskSchedulerClassTest.TestSource testSource
        = new TaskSchedulerClassTest.TestSource(inputKey[0]);
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink(inputKey[0]);

    ComputeGraphBuilder builder = ComputeGraphBuilder.newBuilder(Config.newBuilder().build());
    builder.addSource("source", testSource, parallel);
    ComputeConnection sinkConnection = builder.addSink("sink", testSink, parallel);

    sinkConnection.direct("source")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    builder.setMode(OperationMode.BATCH);

    ComputeGraph graph = builder.build();
    graph.setGraphName(graphName);
    return graph;
  }

  private ComputeGraph createGraph(int parallel, String graphName) {

    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    ComputeGraphBuilder builder = ComputeGraphBuilder.newBuilder(Config.newBuilder().build());
    builder.addSource("source", testSource, parallel);
    ComputeConnection sinkConnection = builder.addSink("sink", testSink, parallel);

    sinkConnection.direct("source")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);

    builder.setMode(OperationMode.BATCH);
    ComputeGraph graph = builder.build();
    graph.setGraphName(graphName);
    return graph;
  }

  private ComputeGraph createGraphWithDifferentParallelism(int parallel, String graphName) {

    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    ComputeGraphBuilder builder = ComputeGraphBuilder.newBuilder(Config.newBuilder().build());
    builder.addSource("source", testSource, parallel);
    ComputeConnection sinkConnection = builder.addSink("sink", testSink, parallel);

    sinkConnection.direct("source")
        .viaEdge(Context.TWISTER2_DIRECT_EDGE)
        .withDataType(MessageTypes.OBJECT);
    builder.setMode(OperationMode.BATCH);

    ComputeGraph graph = builder.build();
    graph.setGraphName(graphName);
    return graph;
  }

  private ComputeGraph createGraphWithComputeTaskAndConstraints(int parallel, String graphName) {

    TaskSchedulerClassTest.TestSource testSource = new TaskSchedulerClassTest.TestSource();
    TaskSchedulerClassTest.TestCompute testCompute = new TaskSchedulerClassTest.TestCompute();
    TaskSchedulerClassTest.TestSink testSink = new TaskSchedulerClassTest.TestSink();

    ComputeGraphBuilder builder = ComputeGraphBuilder.newBuilder(Config.newBuilder().build());
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

    builder.addGraphConstraints(Context.TWISTER2_MAX_TASK_INSTANCES_PER_WORKER, "8");
    ComputeGraph graph = builder.build();
    graph.setGraphName(graphName);
    return graph;
  }
}
