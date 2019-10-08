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
import java.util.logging.Logger;

import com.google.protobuf.Any;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.JobListener;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.task.cdfw.CDFWEnv;

/**
 * This is an implementation of IWorker to support easy deployment of task graphs.
 */
public class CDFWWorker implements IWorker, JobListener {
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
  protected CDFWRuntime cdfwRuntime;

  protected WorkerEnvironment env;

  protected CDFWEnv cdfwEnv;

  @Override
  public void execute(Config cfg, int workerID,
                      IWorkerController wController, IPersistentVolume pVolume,
                      IVolatileVolume vVolume) {
    this.config = cfg;
    this.workerId = workerID;
    this.workerController = wController;
    this.persistentVolume = pVolume;
    this.volatileVolume = vVolume;

    this.env = WorkerEnvironment.init(cfg, workerID, wController, pVolume, vVolume);
    // create the executor
    cdfwRuntime = new CDFWRuntime(cfg, workerID, env.getWorkerList(), env.getCommunicator());
    // register driver listener
    JMWorkerAgent.addJobListener(this);

    // call execute
    execute();

    // lets terminate the network
    env.close();
  }

  /**
   * A user needs to implement this method to create the task graph and execute it
   */
  public void execute() {
    cdfwRuntime.execute();
  }

  @Override
  public void workersScaledUp(int instancesAdded) {
    env.close();
    env = WorkerEnvironment.init(config, workerId, workerController,
        persistentVolume, volatileVolume);
    // create the executor
    //cdfwRuntime = new CDFWRuntime(config, workerId, env.getWorkerList(), env.getCommunicator());
    execute();
  }

  @Override
  public void workersScaledDown(int instancesRemoved) {
    env.close();
    env = WorkerEnvironment.init(config, workerId, workerController,
        persistentVolume, volatileVolume);
    // create the executor
    //cdfwRuntime = new CDFWRuntime(config, workerId, env.getWorkerList(), env.getCommunicator());
    execute();
  }

  @Override
  public void driverMessageReceived(Any anyMessage) {
    cdfwRuntime.driverMessageReceived(anyMessage);
  }

  @Override
  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {
    cdfwRuntime.allWorkersJoined(workerList);
  }
}
