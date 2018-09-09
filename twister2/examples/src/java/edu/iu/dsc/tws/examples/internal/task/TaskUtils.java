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
package edu.iu.dsc.tws.examples.internal.task;

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.threading.Executor;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler;

public final class TaskUtils {
  private TaskUtils() {
  }

  public static void executeBatch(Config config, AllocatedResources resources,
                                  DataFlowTaskGraph graph, IWorkerController workerController) {
    RoundRobinTaskScheduler roundRobinTaskScheduler = new RoundRobinTaskScheduler();
    roundRobinTaskScheduler.initialize(config);

    WorkerPlan workerPlan = createWorkerPlan(resources);
    TaskSchedulePlan taskSchedulePlan = roundRobinTaskScheduler.schedule(graph, workerPlan);

    TWSChannel network = Network.initializeChannel(config, workerController, resources);
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(resources,
        new Communicator(config, network));
    ExecutionPlan plan = executionPlanBuilder.build(config, graph, taskSchedulePlan);
    Executor executor = new Executor(config, resources.getWorkerId(),
        plan, network, OperationMode.BATCH);
    executor.execute();
  }

  public static void execute(Config config, AllocatedResources resources, DataFlowTaskGraph graph,
                             IWorkerController workerController) {
    RoundRobinTaskScheduler roundRobinTaskScheduler = new RoundRobinTaskScheduler();
    roundRobinTaskScheduler.initialize(config);

    WorkerPlan workerPlan = createWorkerPlan(resources);
    TaskSchedulePlan taskSchedulePlan = roundRobinTaskScheduler.schedule(graph, workerPlan);

    TWSChannel network = Network.initializeChannel(config, workerController, resources);
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(resources,
        new Communicator(config, network));
    ExecutionPlan plan = executionPlanBuilder.build(config, graph, taskSchedulePlan);
    Executor executor = new Executor(config, resources.getWorkerId(), plan, network);
    executor.execute();
  }

  public static WorkerPlan createWorkerPlan(AllocatedResources resourcePlan) {
    List<Worker> workers = new ArrayList<>();
    for (WorkerComputeResource resource : resourcePlan.getWorkerComputeResources()) {
      Worker w = new Worker(resource.getId());
      workers.add(w);
    }

    return new WorkerPlan(workers);
  }
}
