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
package edu.iu.dsc.tws.api.cdfw;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

import com.google.protobuf.Any;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.driver.IDriver;
import edu.iu.dsc.tws.common.driver.IDriverMessenger;
import edu.iu.dsc.tws.common.driver.IScaler;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public abstract class BaseDriver implements IDriver {
  private static final Logger LOG = Logger.getLogger(BaseDriver.class.getName());

  /**
   * Execution environment
   */
  private CDFWEnv executionEnv;

  /**
   * This queue is only used to keep the events relating to driver
   */
  private BlockingQueue<List<JobMasterAPI.WorkerInfo>> driverQueue = new LinkedBlockingDeque<>();

  /**
   * Current driver state
   */
  private DriverState driverState = DriverState.WAIT_FOR_WORKERS_TO_START;

  // there is no guarantee that execute will be called after workers joined. So, any message
  // coming from there needs to be put into the driverQueue. After the driver is initialized,
  // events will be handed over to the CDFWExecutor
  @Override
  public void execute(Config config, IScaler scaler, IDriverMessenger messenger) {

    // wait for the cluster to initialize
    List<JobMasterAPI.WorkerInfo> workers = waitForDriverInit();

    // init cdfw env
    this.executionEnv = new CDFWEnv(config, scaler, messenger, workers);

    // call the execute method of the implementation
    execute(executionEnv);

    // now lets close
    executionEnv.close();
  }

  public abstract void execute(CDFWEnv exec);

  @Override
  public void workerMessageReceived(Any anyMessage, int senderWorkerID) {
    this.executionEnv.workerMessageReceived(anyMessage, senderWorkerID);
  }

  @Override
  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {
    if (driverState != DriverState.WAIT_FOR_WORKERS_TO_START) {
      // if the driver is in the 'wait got workers' state, insert the events to the driver's queue
      this.executionEnv.allWorkersJoined(workerList);
    } else {
      // else hand the events over to the executor through the exec env
      try {
        this.driverQueue.put(workerList);
      } catch (InterruptedException e) {
        throw new RuntimeException("Failed to insert the event", e);
      }
    }
  }

  private List<JobMasterAPI.WorkerInfo> waitForDriverInit() {
    // waiting till the workers join the cluster
    try {
      List<JobMasterAPI.WorkerInfo> workers = this.driverQueue.take();

      this.driverState = DriverState.INITIALIZE;

      return workers;
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to take events from the queue", e);
    }

  }
}
