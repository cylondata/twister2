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
package edu.iu.dsc.tws.examples.comms;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkResultsRecorder;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.utils.bench.TimingUnit;
import edu.iu.dsc.tws.examples.verification.ExperimentData;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_SEND;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_SEND;

public abstract class BenchWorker implements Twister2Worker {

  private static final Logger LOG = Logger.getLogger(BenchWorker.class.getName());

  protected int workerId;

  protected LogicalPlan logicalPlan;

  protected JobParameters jobParameters;

  protected final Map<Integer, Boolean> finishedSources = new ConcurrentHashMap<>();

  protected boolean sourcesDone = false;

  protected ExperimentData experimentData;

  //for verification
  protected int[] inputDataArray;
  private boolean verified = true;

  //to capture benchmark results
  protected BenchmarkResultsRecorder resultsRecorder;

  private long streamWait = 0;

  private WorkerEnvironment workerEnv;

  @Override
  public void execute(WorkerEnvironment workerEnvironment) {

    this.workerEnv = workerEnvironment;
    workerId = workerEnv.getWorkerId();
    Config cfg = workerEnv.getConfig();
    Timing.setDefaultTimingUnit(TimingUnit.NANO_SECONDS);

    // create the job parameters
    this.jobParameters = JobParameters.build(cfg);

    this.resultsRecorder = new BenchmarkResultsRecorder(
        cfg,
        workerId == 0
    );

    // lets create the task plan
    this.logicalPlan = Utils.createStageLogicalPlan(workerEnv, jobParameters.getTaskStages());

    //todo collect experiment data : will be removed
    experimentData = new ExperimentData();
    if (jobParameters.isStream()) {
      experimentData.setOperationMode(OperationMode.STREAMING);
    } else {
      experimentData.setOperationMode(OperationMode.BATCH);
    }
    //todo above will be removed

    this.inputDataArray = generateData();

    //todo below will be removed
    experimentData.setInput(this.inputDataArray);
    experimentData.setTaskStages(jobParameters.getTaskStages());
    experimentData.setIterations(jobParameters.getIterations());
    //todo above will be removed

    // now lets execute
    compute(workerEnv);
    // now communicationProgress
    progress();
    // wait for the sync
    try {
      workerEnv.getWorkerController().waitOnBarrier();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException, () -> timeoutException.getMessage());
    }
    // let allows the specific example to close
    close();
    // lets terminate the communicator
    workerEnv.close();
  }

  protected abstract void compute(WorkerEnvironment wEnv);

  protected void progress() {
    // we need to progress the communication

    boolean needProgress = true;

    while (true) {
      boolean seemsDone = !needProgress && isDone();
      if (seemsDone) {
        if (jobParameters.isStream()) {
          if (streamWait == 0) {
            streamWait = System.currentTimeMillis();
          }
          if (streamWait > 0 && (System.currentTimeMillis() - streamWait) > 5000) {
            break;
          }
        } else {
          break;
        }
      } else {
        streamWait = 0;
      }
      // communicationProgress the channel
      workerEnv.getChannel().progress();
      // we should communicationProgress the communication directive
      needProgress = progressCommunication();
    }

    LOG.info(() -> workerId + " FINISHED PROGRESS");
  }

  protected abstract boolean progressCommunication();

  protected abstract boolean isDone();

  protected abstract boolean sendMessages(int task, Object data, int flag);

  public void close() {
  }

  protected void finishCommunication(int src) {
  }

  protected int[] generateData() {
    return DataGenerator.generateIntData(jobParameters.getSize());
  }

  /**
   * This method will verify results and append the output to the results recorder
   */
  protected void verifyResults(ResultsVerifier resultsVerifier, Object results,
                               Map<String, Object> args) {
    if (jobParameters.isDoVerify()) {
      verified = verified && resultsVerifier.verify(results, args);
      //this will record verification failed if any of the iteration fails to verify
      this.resultsRecorder.recordColumn("Verified", verified);
    } else {
      this.resultsRecorder.recordColumn("Verified", "Not Performed");
    }
  }

  protected class MapWorker implements Runnable {
    private int task;

    private boolean timingCondition;

    private boolean timingForLowestTargetOnly = false;

    public MapWorker(int task) {
      this.task = task;
      this.timingCondition = workerId == 0 && task == 0;
      Timing.defineFlag(
          TIMING_MESSAGE_SEND,
          jobParameters.getIterations(),
          this.timingCondition
      );
    }

    public void setTimingForLowestTargetOnly(boolean timingForLowestTargetOnly) {
      this.timingForLowestTargetOnly = timingForLowestTargetOnly;
    }

    @Override
    public void run() {
      LOG.info(() -> "Starting map worker: " + workerId + " task: " + task);

      for (int i = 0; i < jobParameters.getTotalIterations(); i++) {
        // lets generate a message
        if (i == jobParameters.getWarmupIterations()) {
          Timing.mark(TIMING_ALL_SEND, this.timingCondition);
        }

        if (i >= jobParameters.getWarmupIterations()) {
          Timing.mark(TIMING_MESSAGE_SEND,
              this.timingCondition
                  && (!this.timingForLowestTargetOnly
                  || i % jobParameters.getTaskStages().get(1) == 0));
        }

        sendMessages(task, inputDataArray, 0);
      }

      LOG.info(() -> String.format("%d Done sending", workerId));

      synchronized (finishedSources) {
        finishedSources.put(task, true);
        boolean allDone = !finishedSources.values().contains(false);
        finishCommunication(task);
        sourcesDone = allDone;
      }
    }
  }
}
