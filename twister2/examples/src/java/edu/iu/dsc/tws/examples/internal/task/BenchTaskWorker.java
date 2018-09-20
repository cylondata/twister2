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
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.basic.comms.JobParameters;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;

public abstract class BenchTaskWorker extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(BenchTaskWorker.class.getName());

  private DataFlowTaskGraph dataFlowTaskGraph;

  private ExecutionPlan executionPlan;

  private static String source;

  private static String sink;

  private static String edge;

  private static int parallelSource;

  private static int parallelSink;

  private Op op;

  private DataType dataType;

  protected JobParameters jobParameters;

  @Override
  public void execute() {
    LOG.info("Executing ============================");
    intialize();
    GeneratorTask g = new GeneratorTask();
    RecevingTask r = new RecevingTask();
    TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
    graphBuilder.addSource(source, g, parallelSource);
    ComputeConnection computeConnection = graphBuilder.addSink(sink, r, parallelSink);
    run(computeConnection);
    graphBuilder.setMode(OperationMode.BATCH);
    DataFlowTaskGraph graph = graphBuilder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);
    dataFlowTaskGraph = graph;
    executionPlan = plan;
    taskExecutor.execute(dataFlowTaskGraph, executionPlan);
  }

  public void initialize(String src, String target, String e, int pSource,
                         int pSink, Op o, DataType dt) {

    source = src;
    sink = target;
    edge = e;
    parallelSource = pSource;
    parallelSink = pSink;
    op = o;
    dataType = dt;
  }


  public WorkerPlan createWorkerPlan(AllocatedResources resourcePlan) {
    List<Worker> workers = new ArrayList<>();
    for (WorkerComputeResource resource : resourcePlan.getWorkerComputeResources()) {
      Worker w = new Worker(resource.getId());
      workers.add(w);
    }

    return new WorkerPlan(workers);
  }

  private static class GeneratorTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;

    @Override
    public void execute() {
      int[] val = {1};
      if (count == 999) {
        if (context.writeEnd(edge, val)) {
          count++;
        }
      } else if (count < 999) {
        if (context.write(edge, val)) {
          count++;
        }
      }
    }
  }

  private static class RecevingTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      count++;
      if (count % 1 == 0) {
        Object object = message.getContent();
        if (object instanceof int[]) {
          LOG.info("Received Message : " + Arrays.toString((int[]) object));
        }
      }

      return true;
    }
  }

  public abstract void intialize();

  public abstract void run(ComputeConnection computeConnection);



}
