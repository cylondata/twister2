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
package edu.iu.dsc.tws.api.task.ftolerance;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.api.task.TaskExecutor;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.exceptions.TimeoutException;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class CheckpointingWorkerEnv {
  private static final Logger LOG = Logger.getLogger(CheckpointingWorkerEnv.class.getName());


  private TWSChannel channel;

  /**
   * Communicator
   */
  private Communicator communicator;

  /**
   * This id
   */
  private int workerId;

  /**
   * Controller
   */
  private IWorkerController workerController;

  /**
   * Persistent volume
   */
  private IPersistentVolume persistentVolume;

  /**
   * Volatile volume
   */
  private IVolatileVolume volatileVolume;

  /**
   * Configuration
   */
  private Config config;

  /**
   * The task executor to be used
   */
  private TaskExecutor taskExecutor;

  public CheckpointingWorkerEnv(Config cfg, int workerID,
                                IWorkerController wController, IPersistentVolume pVolume,
                                IVolatileVolume vVolume) {
    this.workerId = workerID;
    this.workerController = wController;
    this.persistentVolume = pVolume;
    this.volatileVolume = vVolume;
    this.config = cfg;

    List<JobMasterAPI.WorkerInfo> workerInfoList = null;
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
    taskExecutor = new TaskExecutor(config, workerId, workerInfoList,
        communicator, wController.getCheckpointingClient());
  }
}
