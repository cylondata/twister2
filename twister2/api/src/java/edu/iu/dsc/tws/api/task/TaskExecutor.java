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
package edu.iu.dsc.tws.api.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.dataset.DataSet;
import edu.iu.dsc.tws.dataset.Partition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.threading.Executor;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler;

/**
 * The task executor API
 */
public class TaskExecutor {
  /**
   * Configuration
   */
  private Config config;

  /**
   * Worker id
   */
  private int workerID;

  /**
   * The allocated resources
   */
  private AllocatedResources allocResources;

  /**
   * The network communicator
   */
  private Communicator communicator;

  /**
   * Inputs
   */
  private Map<String, DataSet<Object>> inputs = new HashMap<>();

  public TaskExecutor(Config cfg, int wId, AllocatedResources resources, Communicator net) {
    this.config = cfg;
    this.workerID = wId;
    this.allocResources = resources;
    this.communicator = net;
  }

  /**
   * Execute the task graph and return a data set
   * @param graph task graph
   * @return the data set
   */
  public ExecutionPlan plan(DataFlowTaskGraph graph) {
    RoundRobinTaskScheduler roundRobinTaskScheduler = new RoundRobinTaskScheduler();
    roundRobinTaskScheduler.initialize(config);

    WorkerPlan workerPlan = createWorkerPlan(allocResources);
    TaskSchedulePlan taskSchedulePlan = roundRobinTaskScheduler.schedule(graph, workerPlan);

    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(
        allocResources, communicator);
    return executionPlanBuilder.build(config, graph, taskSchedulePlan);
  }

  public void execute(DataFlowTaskGraph graph, ExecutionPlan plan) {
    Executor executor = new Executor(config, plan, communicator.getChannel(),
        OperationMode.BATCH);
    executor.execute();
  }

  /**
   * Add input to the the task instances
   * @param graph task graph
   * @param plan execution plan
   * @param taskName task name
   * @param inputKey inputkey
   * @param input input
   */
  public void addInput(DataFlowTaskGraph graph, ExecutionPlan plan,
                       String taskName, String inputKey, DataSet<Object> input) {
    Map<Integer, INodeInstance> nodes = plan.getNodes(taskName);
    if (nodes == null) {
      throw new RuntimeException("Failed to get output from non-existing task name: " + taskName);
    }

    for (Map.Entry<Integer, INodeInstance> e : nodes.entrySet()) {
      INodeInstance node = e.getValue();
      if (node instanceof Receptor) {
        ((Receptor) node).add(inputKey, input);
      } else {
        throw new RuntimeException("Cannot add input to non input instance: " + node);
      }
    }
  }

  /**
   * Extract output from a task graph
   * @param graph
   * @param plan
   * @param name
   * @return
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public DataSet<Object> getOutput(DataFlowTaskGraph graph, ExecutionPlan plan, String name) {
    Map<Integer, INodeInstance> nodes = plan.getNodes(name);
    if (nodes == null) {
      throw new RuntimeException("Failed to get output from non-existing task name: " + name);
    }

    DataSet<Object> dataSet = new DataSet<>(0);
    for (Map.Entry<Integer, INodeInstance> e : nodes.entrySet()) {
      INodeInstance node = e.getValue();
      if (node instanceof Collector) {
        Partition partition = ((Collector) node).get();
        dataSet.addPartition(partition);
      } else {
        throw new RuntimeException("Cannot collect from node because it is not a collector: "
            + node);
      }
    }
    return dataSet;
  }

  private WorkerPlan createWorkerPlan(AllocatedResources resourcePlan) {
    List<Worker> workers = new ArrayList<>();
    for (WorkerComputeResource resource : resourcePlan.getWorkerComputeResources()) {
      Worker w = new Worker(resource.getId());
      workers.add(w);
    }

    return new WorkerPlan(workers);
  }

  public void addInput(String key, DataSet<Object> input) {
    inputs.put(key, input);
  }
}
