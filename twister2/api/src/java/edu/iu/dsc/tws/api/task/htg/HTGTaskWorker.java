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
package edu.iu.dsc.tws.api.task.htg;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.exceptions.TimeoutException;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * This is an implementation of IWorker to support easy deployment of task graphs.
 */
public class HTGTaskWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(HTGTaskWorker.class.getName());

  /**
   * The channel
   */
  protected TWSChannel channel;

  /**
   * Communicator
   */
  protected Communicator communicator;

  /**
   * This id
   */
  protected int workerId;

  /**
   * Controller
   */
  protected IWorkerController workerController;

  /**
   * Persistent volume
   */
  protected IPersistentVolume persistentVolume;

  /**
   * Volatile volume
   */
  protected IVolatileVolume volatileVolume;

  /**
   * Configuration
   */
  protected Config config;

  /**
   * The task executor to be used
   */
  protected HTGTaskExecutor taskExecutor;

  @Override
  public void execute(Config cfg, int workerID,
                      IWorkerController wController, IPersistentVolume pVolume,
                      IVolatileVolume vVolume) {
    this.config = cfg;
    this.workerId = workerID;
    this.workerController = wController;
    this.persistentVolume = pVolume;
    this.volatileVolume = vVolume;

    List<JobMasterAPI.WorkerInfo> workerInfoList;
    try {
      workerInfoList = wController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }

    // create the channel
    channel = Network.initializeChannel(config, workerController);
    String persistent = null;
    if (vVolume != null && vVolume.getWorkerDirPath() != null) {
      persistent = vVolume.getWorkerDirPath();
    }
    // create the communicator
    communicator = new Communicator(config, channel, persistent);
    // create the executor
    taskExecutor = new HTGTaskExecutor(config, workerId, workerInfoList, communicator);
    // register driver listener
    JMWorkerAgent.addJobListener(taskExecutor);

    // call execute
    execute();
    // wait for the sync
    try {
      workerController.waitOnBarrier();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
    }
    // lets terminate the network
    communicator.close();
  }

  /**
   * A user needs to implement this method to create the task graph and execute it
   */
  public void execute() {
    taskExecutor.execute();
  }
}
