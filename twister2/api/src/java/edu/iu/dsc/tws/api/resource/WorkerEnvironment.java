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

package edu.iu.dsc.tws.api.resource;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.util.CommonThreadPool;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * Worker environment encapsulating the details about the workers.
 */
public final class WorkerEnvironment {
  private static final Logger LOG = Logger.getLogger(WorkerEnvironment.class.getName());

  /**
   * Configuration of the worker
   */
  private Config config;

  /**
   * Worker id
   */
  private int workerId;

  /**
   * Worker controller
   */
  private IWorkerController workerController;

  /**
   * Persistent storage
   */
  private IPersistentVolume persistentVolume;

  /**
   * Volatile storage, this will be erased after job finished
   */
  private IVolatileVolume volatileVolume;

  /**
   * The communication channel
   */
  private Communicator communicator;

  /**
   * The underlying channel
   */
  private TWSChannel channel;

  /**
   * The worker list we got from discovery
   */
  private final List<JobMasterAPI.WorkerInfo> workerList;

  /**
   * Singleton environment
   */
  private static volatile WorkerEnvironment workerEnv;

  private WorkerEnvironment(Config config, int workerId, IWorkerController workerController,
                            IPersistentVolume persistentVolume, IVolatileVolume volatileVolume) {
    this.config = config;
    this.workerId = workerId;
    this.workerController = workerController;
    this.persistentVolume = persistentVolume;
    this.volatileVolume = volatileVolume;

    //initialize common thread pool
    CommonThreadPool.init(config);

    //wait for the workers to join
    try {
      this.workerList = workerController.getAllWorkers();
    } catch (TimeoutException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      throw new RuntimeException("Unable to get the worker list", e);
    }

    // create the channel
    this.channel = Network.initializeChannel(config, workerController);
    // create the communicator
    this.communicator = new Communicator(config, channel);
  }

  public Config getConfig() {
    return config;
  }

  public int getWorkerId() {
    return workerId;
  }

  public int getNumberOfWorkers() {
    return this.workerController.getNumberOfWorkers();
  }

  public List<JobMasterAPI.WorkerInfo> getWorkerList() {
    return this.workerList;
  }

  public IWorkerController getWorkerController() {
    return workerController;
  }

  public IPersistentVolume getPersistentVolume() {
    return persistentVolume;
  }

  public IVolatileVolume getVolatileVolume() {
    return volatileVolume;
  }

  public Communicator getCommunicator() {
    return communicator;
  }

  public TWSChannel getChannel() {
    return channel;
  }

  /**
   * Close the worker environment
   */
  public void close() {
    this.communicator.close();
    this.channel.close();
  }

  /**
   * Initialize the worker environment, this is a singleton and every job should call this method
   *
   * @param config configuration
   * @param workerId this worker id
   * @param workerController worker controller
   * @param persistentVolume persistent volume
   * @param volatileVolume volatile volume
   * @return the worker environment
   */
  public static WorkerEnvironment init(Config config, int workerId,
                                       IWorkerController workerController,
                                       IPersistentVolume persistentVolume,
                                       IVolatileVolume volatileVolume) {
    if (workerEnv == null) {
      synchronized (WorkerEnvironment.class) {
        if (workerEnv == null) {
          workerEnv = new WorkerEnvironment(config, workerId, workerController, persistentVolume,
              volatileVolume);
        }
      }
    }
    return workerEnv;
  }
}
