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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.examples.verification.ExperimentData;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.graph.OperationMode;

public abstract class BenchWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(BenchWorker.class.getName());

  private Lock lock = new ReentrantLock();

  protected AllocatedResources resourcePlan;

  protected int workerId;

  protected Config config;

  protected TaskPlan taskPlan;

  protected JobParameters jobParameters;

  protected TWSChannel channel;

  protected Communicator communicator;

  protected Map<Integer, Boolean> finishedSources = new ConcurrentHashMap<>();

  protected boolean sourcesDone = false;

  protected List<WorkerNetworkInfo> workerList = null;

  protected ExperimentData experimentData;

  @Override
  public void execute(Config cfg, int workerID, AllocatedResources allocatedResources,
                      IWorkerController workerController, IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    // create the job parameters
    this.jobParameters = JobParameters.build(cfg);
    this.config = cfg;
    this.resourcePlan = allocatedResources;
    this.workerId = workerID;
    this.workerList = workerController.waitForAllWorkersToJoin(50000);
    for (WorkerNetworkInfo w : workerList) {
      LOG.log(Level.INFO, "WorkerNetworkInfo: " + w);
    }
    // lets create the task plan
    this.taskPlan = Utils.createStageTaskPlan(
        cfg, allocatedResources, jobParameters.getTaskStages(), workerList);
    // create the channel
    channel = Network.initializeChannel(config, workerController, resourcePlan);
    // create the communicator
    communicator = new Communicator(cfg, channel);
    //collect experiment data
    experimentData = new ExperimentData();

    // now lets execute
    execute();
    // now communicationProgress
    progress();
    // wait for the sync
    workerController.waitOnBarrier(SchedulerContext.workerEndSyncWaitTime(config));
    // lets terminate the communicator
    communicator.close();
  }

  protected abstract void execute();

  protected void progress() {
    int count = 0;
    // we need to progress the communication

    while (!isDone()) {
      // communicationProgress the channel
      channel.progress();
      // we should communicationProgress the communication directive
      progressCommunication();
    }
  }

  protected abstract void progressCommunication();

  protected abstract boolean isDone();

  protected abstract boolean sendMessages(int task, Object data, int flag);

  protected void finishCommunication(int src) {
  }

  protected Object generateData() {
    return DataGenerator.generateIntData(jobParameters.getSize());
  }

  protected class MapWorker implements Runnable {
    private int task;

    public MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      LOG.log(Level.INFO, "Starting map worker: " + workerId + " task: " + task);
      Object data = generateData();
      experimentData.setInput(data);
      experimentData.setTaskStages(jobParameters.getTaskStages());
      experimentData.setIterations(jobParameters.getIterations());
      if (jobParameters.isStream()) {
        experimentData.setOperationMode(OperationMode.STREAMING);
      } else {
        experimentData.setOperationMode(OperationMode.BATCH);
      }

      for (int i = 0; i < jobParameters.getIterations(); i++) {
        // lets generate a message
        int flag = 0;
        if (i == jobParameters.getIterations() - 1) {
          flag = MessageFlags.LAST;
        }
        sendMessages(task, data, flag);
      }
      LOG.info(String.format("%d Done sending", workerId));
      lock.lock();
      boolean allDone = true;
      finishedSources.put(task, true);
      for (Map.Entry<Integer, Boolean> e : finishedSources.entrySet()) {
        if (!e.getValue()) {
          allDone = false;
        }
      }
      finishCommunication(task);
      sourcesDone = allDone;
      lock.unlock();
    }
  }
}
