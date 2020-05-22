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
package edu.iu.dsc.tws.master.barrier;

import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.faulttolerance.JobFaultListener;
import edu.iu.dsc.tws.api.resource.InitBarrierListener;
import edu.iu.dsc.tws.master.server.WorkerMonitor;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.BarrierResult;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.BarrierType;

/**
 * implements barrier logic
 */
public class BarrierMonitor implements JobFaultListener {
  private static final Logger LOG = Logger.getLogger(BarrierMonitor.class.getName());

  private WorkerMonitor workerMonitor;

  private LongObject expectedWorkersOnDefault = new LongObject(0);
  private LongObject expectedWorkersOnInit = new LongObject(0);

  private LongObject defaultStartTime = new LongObject(0);
  private LongObject initStartTime = new LongObject(0);

  private LongObject defaultTimeout = new LongObject(0);
  private LongObject initTimeout = new LongObject(0);

  private TreeSet<Integer> defaultWaitList;
  private TreeSet<Integer> initWaitList;

  private InitBarrierListener initBarrierListener;
  private BarrierResponder barrierResponder;

  // this is set when a fault occurs in the job
  // it is reset when the default barrier is broken afterwards
  private boolean faultOccurred = false;

  /**
   * to pass a long variable as a reference parameter to another method
   */
  @SuppressWarnings("VisibilityModifier")
  private class LongObject {
    long number;

    LongObject(long n) {
      number = n;
    }

    @Override
    public String toString() {
      return number + "";
    }
  }

  public BarrierMonitor(WorkerMonitor workerMonitor,
                        InitBarrierListener barrierListener) {

    this.workerMonitor = workerMonitor;
    this.initBarrierListener = barrierListener;

    defaultWaitList = new TreeSet<>();
    initWaitList = new TreeSet<>();
  }

  public void setBarrierResponder(BarrierResponder barrierResponder) {
    this.barrierResponder = barrierResponder;
  }

  public void initDefaultAfterRestart(Set<Integer> initialWorkers,
                                      long timeout,
                                      int expectedWorkers) {

    defaultWaitList.addAll(initialWorkers);
    defaultTimeout.number = timeout;
    expectedWorkersOnDefault.number = expectedWorkers;
    defaultStartTime.number = System.currentTimeMillis();
  }

  /**
   * this method is invoked when JM restarts
   * it gets previous status from ZK
   * @param initialWorkers
   * @param timeout
   * @param expectedWorkers
   */
  public void initInitAfterRestart(Set<Integer> initialWorkers,
                                   long timeout,
                                   int expectedWorkers) {

    initWaitList.addAll(initialWorkers);
    initTimeout.number = timeout;
    expectedWorkersOnInit.number = expectedWorkers;
    initStartTime.number = System.currentTimeMillis();
  }

  @Override
  public void faultOccurred() {
    faultOccurred = true;
  }

  @Override
  public void faultRestored() {
    // nothing to be done
  }

  /**
   * a worker arrived at the default barrier
   * @param workerID
   * @param timeout
   */
  public void arrivedAtDefault(int workerID, long timeout) {
    arrived(workerID,
        timeout,
        defaultWaitList,
        BarrierType.DEFAULT,
        expectedWorkersOnDefault,
        defaultStartTime,
        defaultTimeout);
  }

  /**
   * a worker arrived at the init barrier
   * @param workerID
   * @param timeout
   */
  public void arrivedAtInit(int workerID, long timeout) {
    arrived(workerID,
        timeout,
        initWaitList,
        BarrierType.INIT,
        expectedWorkersOnInit,
        initStartTime,
        initTimeout);
  }

  /**
   * this method is called periodically by Job Master to check for the barrier failure
   * barriers fail for two reasons:
   *   the barrier times out: barrier failure message should be broadcasted to all waiting workers
   *   job becomes faulty: default barrier fails in this case only
   */
  public void checkBarrierFailure() {
    // if an INIT barrier is in progress
    if (initStartTime.number > 0) {
      long delay = System.currentTimeMillis() - initStartTime.number;
      if (delay > initTimeout.number) {
        barrierResponder.barrierFailed(BarrierType.INIT, BarrierResult.TIMED_OUT);
        barrierCompleted(initWaitList, expectedWorkersOnInit, initStartTime, initTimeout);
      }
    }

    // if a default barrier is in progress
    if (defaultStartTime.number > 0) {
      long delay = System.currentTimeMillis() - defaultStartTime.number;
      if (delay > defaultTimeout.number) {
        barrierResponder.barrierFailed(BarrierType.DEFAULT, BarrierResult.TIMED_OUT);
        barrierCompleted(
            defaultWaitList, expectedWorkersOnDefault, defaultStartTime, defaultTimeout);
      }
    }

    // if the job became faulty, terminate the default barrier if there is one
    if (faultOccurred) {
      faultOccurred = false;
      if (!defaultWaitList.isEmpty()) {
        barrierResponder.barrierFailed(BarrierType.DEFAULT, BarrierResult.JOB_FAULTY);
        barrierCompleted(
            defaultWaitList, expectedWorkersOnDefault, defaultStartTime, defaultTimeout);
      }
    }
  }

  /**
   * a worker is removed from the default barrier
   * we expect it to come back later on
   * remove it from the list
   */
  public void removedFromDefault(int workerID) {

    // ignore the barrier message if the workerID is out of range
    if (workerID >= expectedWorkersOnDefault.number) {
      return;
    } else if (workerID < 0) {
      return;
    }

    defaultWaitList.remove(workerID);

    // if this is the last worker to be removed from the barrier
    // clear barrier status
    if (defaultWaitList.isEmpty()) {
      expectedWorkersOnDefault.number = 0;
      defaultStartTime.number = 0;
      defaultTimeout.number = 0;
    }
  }

  /**
   * a worker is removed from the init barrier
   * we expect it to come back later on
   * remove it from the list
   */
  public void removedFromInit(int workerID) {

    // ignore the barrier message if the workerID is out of range
    if (workerID >= expectedWorkersOnDefault.number) {
      return;
    } else if (workerID < 0) {
      return;
    }

    initWaitList.remove(workerID);

    // if this is the last worker to be removed from the barrier
    // clear barrier status
    if (initWaitList.isEmpty()) {
      expectedWorkersOnInit.number = 0;
      initStartTime.number = 0;
      initTimeout.number = 0;
    }
  }

  /**
   * when a barrier completes either with success or failure,
   * we clear its status data
   */
  private void barrierCompleted(TreeSet<Integer> waitList,
                               LongObject expectedWorkers,
                               LongObject startTime,
                               LongObject barrierTimeout) {

    waitList.clear();
    expectedWorkers.number = 0;
    startTime.number = 0;
    barrierTimeout.number = 0;
  }

  private void arrived(int workerID,
                       long timeout,
                       TreeSet<Integer> waitList,
                       BarrierType barrierType,
                       LongObject expectedWorkers,
                       LongObject startTime,
                       LongObject barrierTimeout) {

    // ignore the barrier message if the workerID is out of range
    // todo: this is not supposed to happen
    //       maybe we can send a failure response
    if (workerID < 0) {
      LOG.severe("A worker arrived at the " + barrierType + " barrier with a workerID that is "
          + "less than zero:" + workerID + ". Ignoring this barrier message.");
      return;
    } else if ((expectedWorkers.number > 0 && workerID >= expectedWorkers.number)
        || (expectedWorkers.number == 0 && workerID >= workerMonitor.getNumberOfWorkers())) {
      LOG.severe("A worker arrived at the " + barrierType + " barrier with a workerID that is "
          + "larger than max workerID in the job: " + workerID + ". Ignoring this barrier event.");
      return;
    }

    // if the waitList is empty, a new barrier will be starting
    if (waitList.isEmpty()) {
      LOG.fine("First worker[" + workerID + "] arrived at the " + barrierType + " barrier");

      // numberOfWorkers in a job may change during job execution
      // numberOfWorkersOnBarrier is assigned the value of numberOfWorkers in the job
      // when the first barrier message received
      expectedWorkers.number = workerMonitor.getNumberOfWorkers();

      // initialize the start time
      startTime.number = System.currentTimeMillis();

      // set the barrier timeout
      barrierTimeout.number = timeout;

    } else {
      LOG.fine("Worker[" + workerID + "] arrived at the " + barrierType + " barrier");
    }

    waitList.add(workerID);

    // if all workers arrived at the barrier
    if (waitList.size() == expectedWorkers.number) {

      // if this is INIT barrier
      if (barrierType == BarrierType.INIT && initBarrierListener != null) {
        initBarrierListener.allArrived();
      }

      // send response messages to all workers
      LOG.info("All workers reached the " + barrierType + " barrier: " + expectedWorkers);
      barrierResponder.allArrived(barrierType);

      // clear barrier status data
      barrierCompleted(waitList, expectedWorkers, startTime, barrierTimeout);
    }
  }

}
