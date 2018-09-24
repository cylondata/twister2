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
package edu.iu.dsc.tws.api.task;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public abstract class TaskWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(TaskWorker.class.getName());

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
   * Allocated resources
   */
  protected AllocatedResources allocatedResources;

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
  protected TaskExecutor taskExecutor;

  @Override
  public void execute(Config cfg, int workerID, AllocatedResources allocResources,
                      IWorkerController wController, IPersistentVolume pVolume,
                      IVolatileVolume vVolume) {
    this.config = cfg;
    this.workerId = workerID;
    this.allocatedResources = allocResources;
    this.workerController = wController;
    this.persistentVolume = pVolume;
    this.volatileVolume = vVolume;

    // create the channel
    channel = Network.initializeChannel(config, workerController, allocatedResources);
    String persistent = null;
    if (vVolume != null && vVolume.getWorkerDirPath() != null) {
      persistent = vVolume.getWorkerDirPath();
    }
    // create the communicator
    communicator = new Communicator(config, channel, persistent);
    // create the executor
    taskExecutor = new TaskExecutor(config, workerId, allocatedResources, communicator);
    // call execute
    execute();
    // wait for the sync
    workerController.waitOnBarrier(SchedulerContext.workerEndSyncWaitTime(config));
    // lets terminate the network
    communicator.close();
  }

  /**
   * A user needs to implement this method to create the task graph and execute it
   */
  public abstract void execute();
}
