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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.JobFaultyException;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.api.faulttolerance.JobProgress;
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IVolatileVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.rsched.core.WorkerRuntime;

/**
 * Keep information about a managed environment where workers can get restarted.
 */
public class WorkerManager implements IWorkerFailureListener, IAllJoinedListener {
  private static final Logger LOG = Logger.getLogger(WorkerManager.class.getName());

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
  private int workerID;

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
   * The current retries
   */
  private int retries = 0;

  /**
   * Maximum retries
   */
  private final int maxRetries;

  private Set<Integer> failedWorkers = new TreeSet<>();

  private List<JobMasterAPI.WorkerInfo> restartedWorkers = new LinkedList<>();

  public WorkerManager(Config config,
                       int workerID,
                       IWorkerController workerController,
                       IPersistentVolume persistentVolume,
                       IVolatileVolume volatileVolume,
                       IWorker worker) {
    this.config = config;
    this.workerID = workerID;
    this.workerController = workerController;
    this.persistentVolume = persistentVolume;
    this.volatileVolume = volatileVolume;
    this.managedWorker = worker;

    // we default to three retries
    this.maxRetries = FaultToleranceContext.failureRetries(config);

    WorkerRuntime.addWorkerFailureListener(this);
    WorkerRuntime.addAllJoinedListener(this);
    JobProgressImpl.init();
  }

  /**
   * Start the worker manager
   */
  public void start() {
    while (retries < maxRetries) {

      // if the job is faulty, wait failed workers to restart
      if (JobProgress.isJobFaulty()) {
        waitFailedWorkersToRestart();
        if (failedWorkers.isEmpty()) {
          JobProgressImpl.setJobStatus(JobProgress.JobStatus.RESTARTING);
          LOG.warning("Job moves into RESTARTING stage after a fault.");
        } else {
          // if timed out and not all workers restarted, finish execution
          return;
        }
      }

      if (JobProgress.isJobHealthy()) {

        LOG.info("Waiting on the init barrier before starting IWorker: " + workerID
            + " with restartCount: " + workerController.workerRestartCount());
        try {
          workerController.waitOnInitBarrier();
        } catch (TimeoutException e) {
          throw new Twister2RuntimeException("Could not pass through the init barrier", e);
        }

        LOG.info("StartingWorker: " + workerID
            + " with restartCount: " + workerController.workerRestartCount());
        JobProgressImpl.setJobStatus(JobProgress.JobStatus.EXECUTING);
        JobProgressImpl.increaseWorkerExecuteCount();
        JobProgressImpl.setRestartedWorkers(restartedWorkers);
        try {
          managedWorker.execute(
              config, workerID, workerController, persistentVolume, volatileVolume);
          retries++;
        } catch (JobFaultyException cue) {
          // a worker in the cluster should have failed
          // we will try to re-execute this worker
          JobProgressImpl.setJobStatus(JobProgress.JobStatus.FAULTY);
          LOG.warning("thrown JobFaultyException. Some workers should have failed.");
        }

        // we are still in a good state, so we can stop
        if (JobProgress.isJobHealthy()) {
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

  private void waitFailedWorkersToRestart() {

    long startTime = System.currentTimeMillis();
    long maxWaitTime = FaultToleranceContext.waitTimeForFailedWorkers(config);

    while (!failedWorkers.isEmpty()) {

      long elapsedTime = System.currentTimeMillis() - startTime;
      if (elapsedTime > maxWaitTime) {
        LOG.warning("Waited " + maxWaitTime / 60000
            + " minutes to recover the workers from failure, giving up");
        return;
      }
      // lets sleep a little for avoid spinning
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignore) {
      }
    }
  }

  /**
   * todo: if a worker in the job fails before getting allWorkersJoined event,
   *       there is nothing to be done
   *       that worker should be restarted and it should rejoin.
   * @param workerList
   */
  @Override
  public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {

  }

  @Override
  public void failed(int wID) {

    // set the status to fail and notify
    JobProgressImpl.setJobStatus(JobProgress.JobStatus.FAULTY);
    failedWorkers.add(wID);

    // job is becoming faulty, clear restartedWorkers set
    if (JobProgress.isJobHealthy()) {
      faultOccurred(wID);
    }
  }

  @Override
  public void restarted(JobMasterAPI.WorkerInfo workerInfo) {
    failedWorkers.remove(workerInfo.getWorkerID());
    JobProgressImpl.setJobStatus(JobProgress.JobStatus.FAULTY);

    // job is becoming faulty, clear restartedWorkers set
    if (JobProgress.isJobHealthy()) {
      faultOccurred(workerInfo.getWorkerID());
    }

    restartedWorkers.add(workerInfo);
  }

  /**
   * this method must be called exactly once for each fault
   * if there are multiple faults without restoring back to the normal in the middle,
   * it must be called once only
   */
  private void faultOccurred(int wID) {

    // job is becoming faulty, clear restartedWorkers set
    LOG.warning("A fault occurred. Job moves into the FAULTY stage.");
    restartedWorkers.clear();

    JobProgressImpl.faultOccurred(wID);
  }
}
