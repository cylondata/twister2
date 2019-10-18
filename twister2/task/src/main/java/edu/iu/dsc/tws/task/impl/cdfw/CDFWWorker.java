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
package edu.iu.dsc.tws.task.impl.cdfw;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
//import edu.iu.dsc.tws.api.resource.Network;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * This is an implementation of IWorker to support easy deployment of task graphs.
 */
public class CDFWWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(CDFWWorker.class.getName());

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
  protected CDFWRuntime taskExecutor;

  private List<JobMasterAPI.WorkerInfo> workerInfoList;

  @Override
  public void execute(Config cfg, int workerID,
                      IWorkerController wController, IPersistentVolume pVolume,
                      IVolatileVolume vVolume) {
    this.config = cfg;
    this.workerId = workerID;
    this.workerController = wController;
    this.persistentVolume = pVolume;
    this.volatileVolume = vVolume;

    // create the executor
    taskExecutor = new CDFWRuntime(config, workerID, wController);

    // register driver listener
    JMWorkerAgent.addReceiverFromDriver(taskExecutor);
    JMWorkerAgent.addScalerListener(taskExecutor);
    JMWorkerAgent.addAllJoinedListener(taskExecutor);

    // call execute
    taskExecutor.execute();
    // wait for the sync
    try {
      workerInfoList = wController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }

//    // create the channel
//    channel = Network.initializeChannel(config, workerController);
//    String persistent = null;
//    if (vVolume != null && vVolume.getWorkerDirPath() != null) {
//      persistent = vVolume.getWorkerDirPath();
//    }
//    // create the communicator
//    communicator = new Communicator(config, channel, persistent);
//    // create the executor
//    taskExecutor = new CDFWRuntime(config, workerId, workerInfoList, communicator);
//    // register message receiver and job listener
//    JMWorkerAgent.addReceiverFromDriver(taskExecutor);
//    JMWorkerAgent.addScalerListener(taskExecutor);
//    JMWorkerAgent.addAllJoinedListener(taskExecutor);
//
//    // call execute
//    execute();
//    // wait for the sync
//    try {
//      workerController.waitOnBarrier();
//    } catch (TimeoutException timeoutException) {
//      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
//    }
//    // lets terminate the network
//    communicator.close();
  }

  /**
   * A user needs to implement this method to create the task graph and execute it
   */
  /*public void execute() {
    taskExecutor.execute();
  }*/
}
