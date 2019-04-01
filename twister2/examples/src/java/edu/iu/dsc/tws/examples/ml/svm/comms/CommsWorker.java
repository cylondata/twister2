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
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public abstract class CommsWorker implements IWorker {

  private static final Logger LOG = Logger.getLogger(CommsWorker.class.getName());

  protected int workerId;

  protected Config config;

  protected TaskPlan taskPlan;

  protected SVMJobParameters svmJobParameters;

  protected TWSChannel channel;

  protected Communicator communicator;

  protected final Map<Integer, Boolean> finishedSources = new ConcurrentHashMap<>();

  protected boolean sourcesDone = false;

  protected List<JobMasterAPI.WorkerInfo> workerList = null;

  protected double[][] inputDataArray = null;

  protected int features;

  protected int trainingSamples;

  protected int testingSamples;

  protected int parallelism;

  protected List<Integer> taskStages;

  protected String commsType;

  @Override
  public void execute(Config cfg, int workerID, IWorkerController workerController,
                      IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    this.svmJobParameters = SVMJobParameters.build(cfg);
    this.config = cfg;
    this.workerId = workerID;
    try {
      this.workerList = workerController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }

    // lets create the task plan
    generateTaskStages();

    this.taskPlan = Utils.createStageTaskPlan(cfg, workerID,
        taskStages, workerList);

    // create the channel
    channel = Network.initializeChannel(config, workerController);
    // create the communicator
    communicator = new Communicator(cfg, channel);

    loadSVMData();

    // now lets execute
    execute();
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
    communicator.close();

  }

  protected abstract void execute();

  protected abstract void progressCommunication();

  protected abstract boolean isDone();

  protected abstract boolean sendMessages(int task, Object data, int flag);

  protected void progress() {
    while (true) {
      if (isDone()) {
        break;
      }
      // communicationProgress the channel
      channel.progress();
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

  protected class DataStreamer implements Runnable {

    private int task;

    public DataStreamer(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      LOG.info(() -> "Starting map worker: " + workerId + " task: " + task);
      for (int i = 0; i < inputDataArray.length; i++) {
        int flag = (i == inputDataArray.length - 1) ? MessageFlags.LAST : 0;
        sendMessages(task, inputDataArray[i], flag);
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

  public abstract List<Integer> generateTaskStages();
}
