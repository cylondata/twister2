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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.exceptions.TimeoutException;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkResultsRecorder;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.utils.bench.TimingUnit;
import edu.iu.dsc.tws.examples.verification.ExperimentData;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_ALL_SEND;
import static edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants.TIMING_MESSAGE_SEND;

/**
 * BenchWorker class that works with keyed operations
 */
public abstract class KeyedBenchWorker implements IWorker {

  private static final Logger LOG = Logger.getLogger(KeyedBenchWorker.class.getName());

  protected int workerId;

  protected Config config;

  protected TaskPlan taskPlan;

  protected JobParameters jobParameters;

  protected TWSChannel channel;

  protected Communicator communicator;

  protected final Map<Integer, Boolean> finishedSources = new HashMap<>();

  protected boolean sourcesDone = false;

  protected List<JobMasterAPI.WorkerInfo> workerList = null;

  protected ExperimentData experimentData;

  //for verification
  protected int[] inputDataArray;
  private boolean verified = true;

  //to capture benchmark results
  protected BenchmarkResultsRecorder resultsRecorder;

  private long streamWait = 0;

  @Override
  public void execute(Config cfg, int workerID,
                      IWorkerController workerController, IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    Timing.setDefaultTimingUnit(TimingUnit.NANO_SECONDS);
    this.resultsRecorder = new BenchmarkResultsRecorder(
        cfg,
        workerID == 0
    );

    // create the job parameters
    this.jobParameters = JobParameters.build(cfg);
    this.config = cfg;
    this.workerId = workerID;

    // wait for all workers in this job to join
    try {
      workerList = workerController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }

    // lets create the task plan
    this.taskPlan = Utils.createStageTaskPlan(cfg, workerID,
        jobParameters.getTaskStages(), workerList);

    // create the channel
    channel = Network.initializeChannel(config, workerController);
    // create the communicator
    communicator = new Communicator(cfg, channel);

    this.inputDataArray = DataGenerator.generateIntData(jobParameters.getSize());

    //collect experiment data
    experimentData = new ExperimentData();
    // now lets execute
    execute();
    // now progress
    progress();
    // wait for the sync
    try {
      workerController.waitOnBarrier();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
    }
    // let allows the specific example to close
    close();
    // lets terminate the communicator
    communicator.close();
  }

  protected abstract void execute();

  protected void progress() {
    // we need to progress the communication
    while (true) {
      if (jobParameters.isStream()) {
        if (isDone() && streamWait == 0) {
          streamWait = System.currentTimeMillis();
        }
        if (isDone() && streamWait > 0 && (System.currentTimeMillis() - streamWait) > 5000) {
          break;
        }
      } else {
        break;
      }
      // progress the channel
      channel.progress();
      // we should progress the communication directive
      progressCommunication();
    }
  }

  public void close() {
  }

  protected abstract void progressCommunication();

  protected abstract boolean isDone();

  protected abstract boolean sendMessages(int task, Object key, Object data, int flag);

  protected void finishCommunication(int src) {
  }

  protected class MapWorker implements Runnable {

    private final boolean timingCondition;
    private int task;

    public MapWorker(int task) {
      this.task = task;
      this.timingCondition = workerId == 0 && task == 0;
      Timing.defineFlag(
          TIMING_MESSAGE_SEND,
          jobParameters.getIterations(),
          this.timingCondition
      );
    }

    @Override
    public void run() {
      LOG.info(() -> "Starting map worker: " + workerId + " task: " + task);
      experimentData.setInput(inputDataArray);
      experimentData.setTaskStages(jobParameters.getTaskStages());

      Integer key;
      for (int i = 0; i < jobParameters.getTotalIterations(); i++) {
        // lets generate a message
        key = (i + task + 1000) % jobParameters.getTaskStages().get(1);
        int flag = i == jobParameters.getTotalIterations() - 1 ? MessageFlags.LAST : 0;

        if (i == jobParameters.getWarmupIterations()) {
          Timing.mark(TIMING_ALL_SEND, this.timingCondition);
        }

        if (i >= jobParameters.getWarmupIterations()) {
          Timing.mark(TIMING_MESSAGE_SEND, this.timingCondition);
        }

        sendMessages(task, key, inputDataArray, flag);
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
