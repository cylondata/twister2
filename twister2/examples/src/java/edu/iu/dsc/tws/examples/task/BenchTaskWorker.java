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

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.examples.comms.DataGenerator;
import edu.iu.dsc.tws.examples.comms.JobParameters;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkResultsRecorder;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.utils.bench.TimingUnit;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.IExecution;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_SEND;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_SEND;

public abstract class BenchTaskWorker extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(BenchTaskWorker.class.getName());

  protected static final String SOURCE = "source";

  protected static final String SINK = "sink";

  protected static final int LOWEST_FLAG = -1;

  protected DataFlowTaskGraph dataFlowTaskGraph;

  protected TaskGraphBuilder taskGraphBuilder;

  protected ExecutionPlan executionPlan;

  protected ComputeConnection computeConnection;

  protected static JobParameters jobParameters;

  protected static int[] inputDataArray;

  //to capture benchmark results
  protected static BenchmarkResultsRecorder resultsRecorder;

  protected static AtomicInteger sendersInProgress = new AtomicInteger();
  protected static AtomicInteger receiversInProgress = new AtomicInteger();

  @Override
  public void execute() {
    if (resultsRecorder == null) {
      resultsRecorder = new BenchmarkResultsRecorder(
          config,
          workerId == 0
      );
    }
    Timing.setDefaultTimingUnit(TimingUnit.NANO_SECONDS);
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
    IExecution execution = taskExecutor.iExecute(dataFlowTaskGraph, executionPlan);

    while (execution.progress() && (sendersInProgress.get() != 0
        || receiversInProgress.get() != 0)) {
      //do nothing
      //System.out.println(sendersInProgress.get() + "," + receiversInProgress.get());
    }

    //now just spin for several iterations to progress the remaining communicatoin.
    //todo fix streaming to return false, when comm is done
    long timeNow = System.currentTimeMillis();
    if (jobParameters.isStream()) {
      LOG.info("Streaming Example task will wait 10secs to finish communication...");
      while (System.currentTimeMillis() - timeNow < 10000) {
        execution.progress();
      }
    }
    execution.stop();
    execution.close();
  }

  public abstract TaskGraphBuilder buildTaskGraph();

  /**
   * This method will verify results and append the output to the results recorder
   */
  protected static boolean verifyResults(ResultsVerifier resultsVerifier,
                                         Object results,
                                         Map<String, Object> args,
                                         boolean verified) {
    boolean currentVerifiedStatus = verified;
    if (jobParameters.isDoVerify()) {
      currentVerifiedStatus = verified && resultsVerifier.verify(results, args);
      //this will record verification failed if any of the iteration fails to verify
      resultsRecorder.recordColumn("Verified", verified);
    } else {
      resultsRecorder.recordColumn("Verified", "Not Performed");
    }
    return currentVerifiedStatus;
  }

  public static boolean getTimingCondition(String taskName, TaskContext ctx) {
    Optional<TaskInstancePlan> min = ctx.getTasksInThisWorkerByName(taskName).stream()
        .min(Comparator.comparingInt(TaskInstancePlan::getTaskIndex));
    //do timing only on task having lowest ID
    if (min.isPresent()) {
      return ctx.getWorkerId() == 0
          && ctx.taskIndex() == min.get().getTaskIndex();
    } else {
      LOG.warning("Couldn't find lowest ID task for " + SOURCE);
      return false;
    }
  }

  protected static class SourceTask extends BaseSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;
    private int iterations;
    private boolean timingCondition;
    private boolean keyed = false;

    private boolean endNotified = false;

    // if this is set to true, Timing.mark(TIMING_MESSAGE_SEND, this.timingCondition);
    // will be marked only for the lowest target(sink)
    private boolean markTimingOnlyForLowestTarget = false;
    private int noOfTargets = 0;

    public SourceTask(String e) {
      this.iterations = jobParameters.getIterations() + jobParameters.getWarmupIterations();
      this.edge = e;
    }

    public SourceTask(String e, boolean keyed) {
      this(e);
      this.keyed = keyed;
    }

    public void setMarkTimingOnlyForLowestTarget(boolean markTimingOnlyForLowestTarget) {
      this.markTimingOnlyForLowestTarget = markTimingOnlyForLowestTarget;
    }

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.timingCondition = getTimingCondition(SOURCE, ctx);
      this.noOfTargets = ctx.getTasksByName(SINK).size();
      sendersInProgress.incrementAndGet();
    }

    private void notifyEnd() {
      if (endNotified) {
        return;
      }
      sendersInProgress.decrementAndGet();
      endNotified = true;
      LOG.info(String.format("Source : %d done sending.", context.taskIndex()));
    }

    @Override
    public void execute() {
      if (count < iterations) {
        if (count == jobParameters.getWarmupIterations()) {
          Timing.mark(TIMING_ALL_SEND, this.timingCondition);
        }


        if ((this.keyed && context.write(this.edge, context.taskIndex(), inputDataArray))
            || (!this.keyed && context.write(this.edge, inputDataArray))) {
          count++;
        }

        if (jobParameters.isStream() && count >= jobParameters.getWarmupIterations()) {
          // if we should mark timing only for lowest target, consider that for timing condition
          boolean sendingToLowestTarget = count % noOfTargets == 0;
          Timing.mark(TIMING_MESSAGE_SEND,
              this.timingCondition
                  && (!this.markTimingOnlyForLowestTarget || sendingToLowestTarget)
          );
        }
      } else {
        context.end(this.edge);
        this.notifyEnd();
      }
    }
  }
}
