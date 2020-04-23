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
package edu.iu.dsc.tws.rsched.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.faulttolerance.Fault;
import edu.iu.dsc.tws.api.faulttolerance.FaultAcceptable;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IManagedFailureListener;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * Keep information about a managed environment where workers can get restarted.
 */
public class WorkerManager implements IManagedFailureListener, IAllJoinedListener {
  private static final Logger LOG = Logger.getLogger(WorkerManager.class.getName());

  /**
   * Keep track of the components that have the ability to deal with faults
   */
  private List<FaultAcceptable> faultComponents = new ArrayList<>();

  /**
   * The IWorker we are working with
   */
  private IWorker managedWorker;

  /**
   * The configuration
   */
  private Config config;

  /**
   * The worker id
   */
  private int workerId;

  /**
   * The worker controller
   */
  private IWorkerController workerController;

  /**
   * Persistant volume
   */
  private IPersistentVolume persistentVolume;

  /**
   * The volatile volume
   */
  private IVolatileVolume volatileVolume;

  /**
   * The worker status
   */
  private enum WorkerStatus {
    RUNNING,
    FAILED,
    RESTARTED,
  }

  /**
   * Keep track of the status
   */
  private WorkerStatus workerStatus;

  /**
   * The current retries
   */
  private int retries = 0;

  /**
   * Maximum retries
   */
  private final int maxRetries;

  /**
   * The start time of this worker
   */
  private long startTime = 0;

  private long failedTime = 0;

  public WorkerManager(Config config,
                       int workerID,
                       IWorkerController workerController,
                       IPersistentVolume persistentVolume,
                       IVolatileVolume volatileVolume,
                       IWorker worker) {
    this.config = config;
    this.workerId = workerID;
    this.workerController = workerController;
    this.persistentVolume = persistentVolume;
    this.volatileVolume = volatileVolume;
    this.managedWorker = worker;
    // we default to three retries
    this.maxRetries = FaultToleranceContext.failureRetries(config, 3);

//    WorkerRuntime.addWorkerFailureListener(this);
//    WorkerRuntime.addAllJoinedListener(this);
    this.workerStatus = WorkerStatus.RUNNING;
  }

  /**
   * Start the worker manager
   */
  public void start() {
    while (retries < maxRetries) {
      if (workerStatus == WorkerStatus.FAILED) {
        long elapsedTime = System.currentTimeMillis() - failedTime;
        if (elapsedTime > 600000) {
          LOG.info("Waited 10 mins to recover the workers from failre, giving up");
          break;
        }
        // lets sleep a little for avoid spinning
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignore) {
        }
      }

      if (workerStatus == WorkerStatus.RUNNING) {
        managedWorker.execute(config, workerId, workerController, persistentVolume, volatileVolume);
        retries++;
        // we are still in a good state, so we can stop
        if (workerStatus == WorkerStatus.RUNNING) {
          LOG.info("Worker finished successfully");
          break;
        }
      }

      // we break here
      if (retries >= maxRetries) {
        LOG.info(String.format("Retried %d times and failed, we are exiting", retries));
        break;
      }
    }
  }

  /**
   * Register a fault component to this managed envrionment
   *
   * @param fa the fault accepted component
   */
  public void registerFaultComponent(FaultAcceptable fa) {
    faultComponents.add(fa);
  }

  @Override
  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {

  }

  @Override
  public void failed(int workerID) {
    // set the status to fail and notify
    workerStatus = WorkerStatus.FAILED;
    // lets record the failure time
    failedTime = System.currentTimeMillis();
    // lets tell everyone that there is a fault
    for (FaultAcceptable fa : faultComponents) {
      try {
        fa.onFault(new Fault(workerID));
      } catch (Twister2Exception e) {
        LOG.log(Level.WARNING, "Cannot propergate the failure", e);
      }
    }
  }

  @Override
  public void restarted(JobMasterAPI.WorkerInfo workerInfo) {
    // wait until the previous execution is finished
    workerStatus = WorkerStatus.RUNNING;
  }

  @Override
  public void registerFaultAcceptor(FaultAcceptable faultAcceptable) {
    faultComponents.add(faultAcceptable);
  }

  @Override
  public void unRegisterFaultAcceptor(FaultAcceptable faultAcceptable) {
    faultComponents.remove(faultAcceptable);
  }
}
