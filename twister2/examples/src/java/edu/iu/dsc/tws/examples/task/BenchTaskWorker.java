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

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.executor.IExecution;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.nodes.BaseSource;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskInstancePlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.examples.comms.DataGenerator;
import edu.iu.dsc.tws.examples.comms.JobParameters;
import edu.iu.dsc.tws.examples.task.streaming.windowing.data.EventTimeData;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkResultsRecorder;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.utils.bench.TimingUnit;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.task.ComputeEnvironment;
import edu.iu.dsc.tws.task.impl.ComputeConnection;
import edu.iu.dsc.tws.task.impl.ComputeGraphBuilder;
import edu.iu.dsc.tws.task.window.BaseWindowSource;

import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_SEND;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_SEND;

public abstract class BenchTaskWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(BenchTaskWorker.class.getName());

  protected static final String SOURCE = "source";

  protected static final String SINK = "sink";

  protected ComputeGraph computeGraph;

  protected ComputeGraphBuilder computeGraphBuilder;

  protected ExecutionPlan executionPlan;

  protected ComputeConnection computeConnection;

  protected static JobParameters jobParameters;

  protected static int[] inputDataArray;

  //to capture benchmark results
  protected static BenchmarkResultsRecorder resultsRecorder;

  protected static AtomicInteger sendersInProgress = new AtomicInteger();
  protected static AtomicInteger receiversInProgress = new AtomicInteger();

  @Override
  public void execute(Config config, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    ComputeEnvironment cEnv = ComputeEnvironment.init(config, workerID,
        workerController, persistentVolume, volatileVolume);

    if (resultsRecorder == null) {
      resultsRecorder = new BenchmarkResultsRecorder(
          config,
          workerID == 0
      );
    }
    Timing.setDefaultTimingUnit(TimingUnit.NANO_SECONDS);
    jobParameters = JobParameters.build(config);
    computeGraphBuilder = ComputeGraphBuilder.newBuilder(config);
    if (jobParameters.isStream()) {
      computeGraphBuilder.setMode(OperationMode.STREAMING);
    } else {
      computeGraphBuilder.setMode(OperationMode.BATCH);
    }

    inputDataArray = DataGenerator.generateIntData(jobParameters.getSize());

    buildTaskGraph();
    computeGraph = computeGraphBuilder.build();
    executionPlan = cEnv.getTaskExecutor().plan(computeGraph);
    IExecution execution = cEnv.getTaskExecutor().iExecute(computeGraph, executionPlan);

    if (jobParameters.isStream()) {
      while (execution.progress()
          && (sendersInProgress.get() != 0 || receiversInProgress.get() != 0)) {
        //do nothing
      }

      //now just spin for several iterations to progress the remaining communication.
      //todo fix streaming to return false, when comm is done
      long timeNow = System.currentTimeMillis();
      LOG.info("Streaming Example task will wait 10secs to finish communication...");
      while (System.currentTimeMillis() - timeNow < 10000) {
        execution.progress();
      }
    } else {
      while (execution.progress()) {
        //do nothing
      }
    }
    LOG.info("Stopping execution....");
    execution.stop();
    execution.close();
  }

  public abstract ComputeGraphBuilder buildTaskGraph();

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

    protected void notifyEnd() {
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


        if ((this.keyed && context.write(this.edge, count, inputDataArray))
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

  protected static class SourceWindowTask extends BaseWindowSource {
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

    public SourceWindowTask(String e) {
      this.iterations = jobParameters.getIterations() + jobParameters.getWarmupIterations();
      this.edge = e;
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

  protected static class SourceWindowTimeStampTask extends BaseWindowSource {
    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String edge;
    private int iterations;
    private boolean timingCondition;
    private boolean keyed = false;
    private long prevTime = System.currentTimeMillis();

    private boolean endNotified = false;

    // if this is set to true, Timing.mark(TIMING_MESSAGE_SEND, this.timingCondition);
    // will be marked only for the lowest target(sink)
    private boolean markTimingOnlyForLowestTarget = false;
    private int noOfTargets = 0;

    public SourceWindowTimeStampTask(String e) {
      this.iterations = jobParameters.getIterations() + jobParameters.getWarmupIterations();
      this.edge = e;
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
        long timestamp = System.currentTimeMillis();
        EventTimeData eventTimeData = new EventTimeData(inputDataArray, count,
            timestamp);
        prevTime = timestamp;
        if ((this.keyed && context.write(this.edge, context.taskIndex(), eventTimeData))
            || (!this.keyed && context.write(this.edge, eventTimeData))) {
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
