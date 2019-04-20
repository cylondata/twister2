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
package edu.iu.dsc.tws.examples.task;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.examples.comms.DataGenerator;
import edu.iu.dsc.tws.examples.comms.JobParameters;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkResultsRecorder;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.window.BaseWindowSource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public abstract class BenchWindowTaskWorker extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(BenchWindowTaskWorker.class.getName());

  protected static final String SOURCE = "source";

  protected static final String SINK = "sink";

  protected DataFlowTaskGraph dataFlowTaskGraph;

  protected TaskGraphBuilder taskGraphBuilder;

  protected ExecutionPlan executionPlan;

  protected ComputeConnection computeConnection;

  protected static JobParameters jobParameters;

  protected static int[] inputDataArray;

  //to capture benchmark results
  protected static BenchmarkResultsRecorder resultsRecorder;


  @Override
  public void execute() {

    jobParameters = JobParameters.build(config);
    taskGraphBuilder = TaskGraphBuilder.newBuilder(config);
    if (jobParameters.isStream()) {
      taskGraphBuilder.setMode(OperationMode.STREAMING);
    } else {
      taskGraphBuilder.setMode(OperationMode.BATCH);
    }


    inputDataArray = DataGenerator.generateIntData(jobParameters.getSize());

    buildTaskGraph();
    dataFlowTaskGraph = taskGraphBuilder.build();
    executionPlan = taskExecutor.plan(dataFlowTaskGraph);
  }

  public abstract TaskGraphBuilder buildTaskGraph();

  protected static class SourceWindowTask extends BaseWindowSource {

    private static final long serialVersionUID = -6402650835073995738L;

    private int count = 0;
    private String edge;
    private int iterations;

    private boolean endNotified = false;

    public SourceWindowTask(String e) {
      this.edge = e;
      this.iterations = jobParameters.getIterations();
    }

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
    }

    @Override
    public void execute() {
      if (count < iterations) {
        context.write(this.edge, inputDataArray);
      } else {
        context.end(this.edge);
        this.notifyEnd();
      }
    }

    private void notifyEnd() {
      if (endNotified) {
        return;
      }
      endNotified = true;
      LOG.info(String.format("Source : %d done sending.", context.taskIndex()));
    }

  }

}
