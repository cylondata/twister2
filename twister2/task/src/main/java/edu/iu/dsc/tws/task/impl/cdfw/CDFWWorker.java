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

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

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

  @Override
  public void execute(Config cfg, JobAPI.Job job,
                      IWorkerController wController, IPersistentVolume pVolume,
                      IVolatileVolume vVolume) {
    this.config = cfg;
    this.workerId = wController.getWorkerInfo().getWorkerID();
    this.workerController = wController;
    this.persistentVolume = pVolume;
    this.volatileVolume = vVolume;

    // create the executor
    taskExecutor = new CDFWRuntime(config, workerId, wController);

    // register driver listener
    JMWorkerAgent.addReceiverFromDriver(taskExecutor);
    JMWorkerAgent.addScalerListener(taskExecutor);
    JMWorkerAgent.addAllJoinedListener(taskExecutor);

    // call execute
    taskExecutor.execute();
  }
}
