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
package edu.iu.dsc.tws.examples.ml.svm.comms;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public abstract class CommsWorker implements IWorker {

  private static final Logger LOG = Logger.getLogger(CommsWorker.class.getName());
  protected final Map<Integer, Boolean> finishedSources = new ConcurrentHashMap<>();
  protected int workerId;
  protected LogicalPlan logicalPlan;
  protected SVMJobParameters svmJobParameters;
  protected boolean sourcesDone = false;

  protected double[][] inputDataArray = null;

  protected int features;

  protected int trainingSamples;

  protected int testingSamples;

  protected int parallelism;

  protected List<Integer> taskStages;

  protected String commsType;

  private WorkerEnvironment workerEnv;

  @Override
  public void execute(Config cfg, JobAPI.Job job, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    this.svmJobParameters = SVMJobParameters.build(cfg);
    this.workerId = workerController.getWorkerInfo().getWorkerID();

    // create a worker environment
    this.workerEnv = WorkerEnvironment.init(cfg, job, workerController, persistentVolume,
        volatileVolume);

    // lets create the task plan
    generateTaskStages();

    this.logicalPlan = Utils.createStageLogicalPlan(workerEnv, taskStages);

    loadSVMData();

    // now lets execute
    execute(workerEnv);
    // now communicationProgress
    progress();
    // wait for the sync
    try {
      workerController.waitOnBarrier();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException, () -> timeoutException.getMessage());
    }
    // let allows the specific example to close
    close();
    // lets terminate the communicator
    workerEnv.close();

  }

  protected abstract void execute(WorkerEnvironment wEnv);

  protected abstract void progressCommunication();

  protected abstract boolean isDone();

  protected abstract boolean sendMessages(int task, Object data, int flag);

  protected void progress() {
    while (true) {
      if (isDone()) {
        break;
      }
      // communicationProgress the channel
      workerEnv.getChannel().progress();
      // we should communicationProgress the communication directive
      progressCommunication();
    }
  }

  public void close() {
  }

  protected void finishCommunication(int src) {
  }

  private void loadSVMData() {
    this.features = 2;
    this.trainingSamples = 1;
    this.inputDataArray = new double[this.trainingSamples][this.features];
    for (int i = 0; i < this.trainingSamples; i++) {
      Arrays.fill(this.inputDataArray[i], 1.0);
    }
  }

  private void printSampleData() {
    LOG.info(String.format("%s", Arrays.toString(this.inputDataArray[0])));
  }

  public abstract List<Integer> generateTaskStages();

  protected class DataStreamer implements Runnable {

    private int task;

    public DataStreamer(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      LOG.info(() -> "Starting map worker: " + workerId + " task: " + task);
      for (int i = 0; i < inputDataArray.length; i++) {
        sendMessages(task, inputDataArray[i], 0);
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
