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
package edu.iu.dsc.tws.task.impl;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.task.TaskEnvironment;

/**
 * This is an implementation of IWorker to support easy deployment of task graphs.
 */
public abstract class TaskWorker implements IWorker {

  private static final Logger LOG = Logger.getLogger(TaskWorker.class.getName());

  /**
   * Worker environment
   */
  protected WorkerEnvironment workerEnvironment;

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
  protected TaskExecutor taskExecutor;

  protected TaskEnvironment taskEnvironment;

  @Override
  public void execute(Config cfg, int workerID,
                      IWorkerController wController, IPersistentVolume pVolume,
                      IVolatileVolume vVolume) {
    this.config = cfg;
    this.workerId = workerID;
    this.workerController = wController;
    this.persistentVolume = pVolume;
    this.volatileVolume = vVolume;

    workerEnvironment = WorkerEnvironment.init(config, workerID,
        workerController, pVolume, vVolume);

    taskEnvironment = TaskEnvironment.init(workerEnvironment);

    // to keep backward compatibility
    taskExecutor = taskEnvironment.getTaskExecutor();

    // call execute
    execute();
    // wait for the sync
    try {
      workerEnvironment.getWorkerController().waitOnBarrier();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
    }

    taskEnvironment.close();

    // lets terminate the network
    workerEnvironment.close();
    // we are done executing
    LOG.log(Level.FINE, String.format("%d Worker done", workerID));
  }

  /**
   * A user needs to implement this method to create the task graph and execute it
   */
  public abstract void execute();
}
