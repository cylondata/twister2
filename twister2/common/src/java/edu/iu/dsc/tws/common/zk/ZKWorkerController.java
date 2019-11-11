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

package edu.iu.dsc.tws.common.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.ControllerContext;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.IWorkerStatusUpdater;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerState;

/**
 * we assume each worker is assigned a unique ID outside of this class
 * If a worker joins with an ID that already exists in the group,
 * we assume that the worker is coming from failure. It is the same worker.
 * It is very important that there is no worker ID collusion among different workers in the same job
 * <p>
 * We create a persistent znode for the job.
 * Job znode is created with submitting client.
 * We create an ephemeral znode for each worker under the job znode.
 * <p>
 * Each worker znode keeps two pieces of data:
 * WorkerInfo object of the worker that created the ephemeral worker znode
 * status of this worker. Worker status changes during job execution and this field is updated.
 * <p>
 * When workers fail, and their znodes are deleted from ZooKeeper,
 * Their workerInfo objects will be also gone.
 * So, we keep a list of all joined workers in each worker locally.
 * When the worker comes back from a failure,
 * its WorkerInfo is updated in each local worker list.
 * <p>
 * When the job completes, job terminator deletes persistent job znode.
 * Sometimes, job terminator may not be invoked or it may fail before cleaning p job resources.
 * Therefore, when a job is submitted, it is important to check whether there is
 * any existing znode with the same name as the submitted job in ZooKeeper.
 * If so, job submission will fail and first job termination should be invoked.
 * <p>
 * we use a barrier to make all workers wait until the last worker arrives at the barrier point
 * we count the number of waiting workers by using a DistributedAtomicInteger
 */

public class ZKWorkerController extends ZKBaseController
    implements IWorkerController, IWorkerStatusUpdater {

  public static final Logger LOG = Logger.getLogger(ZKWorkerController.class.getName());

  // WorkerInfo object for this worker
  private WorkerInfo workerInfo;

  // persistent ephemeral znode for this worker
  private PersistentNode workerZNode;

  // variables related to the barrier
  private DistributedAtomicInteger daiForBarrier;
  private DistributedBarrier barrier;

  /**
   * Construct ZKWorkerController but not initialize yet
   * @param config
   * @param jobName
   * @param numberOfWorkers
   * @param workerInfo
   */
  public ZKWorkerController(Config config,
                            String jobName,
                            int numberOfWorkers,
                            WorkerInfo workerInfo) {
    super(config, jobName, numberOfWorkers);
    this.workerInfo = workerInfo;
  }

  /**
   * Initialize this ZKWorkerController
   * Connect to the ZK server
   * create an ephemeral znode for this worker
   * set this worker info in the body of that node
   * worker status also goes into the body of that znode
   * initialState has to be either: WorkerState.STARTED or WorkerState.RESTARTED
   *
   * The body of the worker znode will be updated as the status of worker changes
   * from STARTING, RUNNING, COMPLETED
   */
  public void initialize(WorkerState initialState) throws Exception {

    if (!(initialState == WorkerState.STARTED || initialState == WorkerState.RESTARTED)) {
      throw new Exception("initialState has to be either WorkerState.STARTED or "
          + "WorkerState.RESTARTED. Supplied value: " + initialState);
    }

    try {
      super.initialize();

      String barrierPath = ZKUtils.constructBarrierPath(rootPath, jobName);
      barrier = new DistributedBarrier(client, barrierPath);

      String daiPathForBarrier = ZKUtils.constructDaiPathForBarrier(rootPath, jobName);
      daiForBarrier = new DistributedAtomicInteger(client,
          daiPathForBarrier, new ExponentialBackoffRetry(1000, 3));

      createWorkerZnode(initialState);

      LOG.info("This worker: " + workerInfo.getWorkerID() + " initialized successfully.");

    } catch (Exception e) {
//      LOG.log(Level.SEVERE, "Exception when initializing ZKWorkerController", e);
      throw e;
    }
  }

  /**
   * Update worker status with new state
   * return true if successful
   * <p>
   * Initially worker status is set as STARTED or RESTARTED.
   * Therefore, there is no need to call this method after starting this IWorkerController
   * This method should be called to change worker status to COMPLETED, FAILED, etc.
   */
  @Override
  public boolean updateWorkerStatus(WorkerState newStatus) {

    byte[] workerZnodeBody = ZKUtils.encodeWorkerZnode(workerInfo, newStatus.getNumber());

    try {
      client.setData().forPath(workerZNode.getActualPath(), workerZnodeBody);

      // when we do not getData after setData,
      // sometimes the data may not be changed immediately
      // this seems to be working
      workerZnodeBody = client.getData().forPath(workerZNode.getActualPath());
      Pair<WorkerInfo, WorkerState> pair = ZKUtils.decodeWorkerZnode(workerZnodeBody);
      if (pair.getValue() != newStatus) {
        LOG.severe("Could not set worker status. Tried to set it to: " + newStatus
            + ". But worker status is " + pair.getValue());
        return false;
      }

      LOG.info("Worker status changed to: " + newStatus);
      return true;
    } catch (Exception e) {
      LOG.log(Level.SEVERE,
          "Could not update worker status in znode: " + workerInfo.getWorkerID(), e);
      return false;
    }
  }

  @Override
  public WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  /**
   * create the znode for this worker
   */
  private void createWorkerZnode(WorkerState initialState) {
    String workerPath = ZKUtils.constructWorkerEphemPath(jobPath, workerInfo.getWorkerID());

    if (initialState == WorkerState.RESTARTED) {
      // TODO: check whether a znode for this worker exists in the server
      //  if so, delete it first
    }

    // put WorkerInfo and its state into znode body
    byte[] workerZnodeBody = ZKUtils.encodeWorkerZnode(workerInfo, initialState.getNumber());
    workerZNode = ZKUtils.createPersistentEphemeralZnode(workerPath, workerZnodeBody);
    workerZNode.start();
    try {
      workerZNode.waitForInitialCreate(10000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE,
          "Could not create worker znode: " + workerInfo.getWorkerID(), e);
      throw new RuntimeException("Could not create worker znode: " + workerInfo, e);
    }

    String fullWorkerPath = workerZNode.getActualPath();
    LOG.info("An ephemeral znode is created for this worker: " + fullWorkerPath);
  }

  /**
   * Get current list of workers
   * This list does not have the workers that have failed or already left
   */
  public List<WorkerInfo> getCurrentWorkers() {

    List<WorkerInfo> currentWorkers = new ArrayList<>();

    for (ChildData child : childrenCache.getCurrentData()) {
      int id = ZKUtils.getWorkerIDFromPath(child.getPath());
      WorkerInfo worker = getWorkerInfoForID(id);
      if (worker != null) {
        currentWorkers.add(worker);
      }

    }
    return currentWorkers;
  }

  /**
   * get number of current workers in the job as seen from this worker
   */
  public int getNumberOfCurrentWorkers() {
    return childrenCache.getCurrentData().size();
  }

  /**
   * try to increment the daiForBarrier
   * try 100 times if fails
   */
  private boolean incrementBarrierDAI(int tryCount, long timeLimitMilliSec) {

    if (tryCount == 100) {
      return false;
    }

    try {
      AtomicValue<Integer> incremented = daiForBarrier.increment();
      if (incremented.succeeded()) {
        LOG.fine("DistributedAtomicInteger for Barrier increased to: " + incremented.postValue());

        // if this is the last worker to enter, remove the barrier and let all workers be released
        if (incremented.postValue() == numberOfWorkers) {
          barrier.removeBarrier();
          // clear the value to zero, so that new barrier can be used
          daiForBarrier.forceSet(0);
          return true;

          // if this is not the last worker, set the barrier and wait
        } else {
          barrier.setBarrier();
          return barrier.waitOnBarrier(timeLimitMilliSec, TimeUnit.MILLISECONDS);
        }

      } else {
        return incrementBarrierDAI(tryCount + 1, timeLimitMilliSec);
      }
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Failed to increment the DistributedAtomicInteger for Barrier. "
          + "Will try again ...", e);
      return incrementBarrierDAI(tryCount + 1, timeLimitMilliSec);
    }
  }

  /**
   * we use a DistributedAtomicInteger to count the number of workers
   * that have reached to the barrier point.
   * <p>
   * Last worker to call this method increases the DistributedAtomicInteger,
   * removes the barrier and lets all previous waiting workers be released.
   * <p>
   * Other workers to call this method increase the DistributedAtomicInteger,
   * enable the barrier by calling setBarrier method and wait.
   * <p>
   * It is enough to call setBarrier method by only the first worker,
   * however, it does not harm calling by many workers.
   * <p>
   * If we let only the first worker to set the barrier with setBarrier method,
   * then, the second worker may call this method after the dai is increased
   * but before the setBarrier method is called. To prevent this,
   * we may need to use a distributed InterProcessMutex.
   * So, instead of using a distributed InterProcessMutex, we call this method many times.
   * <p>
   * We reset the value of DistributedAtomicInteger to zero after all workers reached the barrier
   * This lets barrier to be used again even if the job is scaled up/down
   * <p>
   * This method may be called many times during a computation.
   * <p>
   * If the job is scaled during some workers are waiting on a barrier,
   * The barrier does not work, since the numberOfWorkers changes
   * <p>
   * if timeout is reached, throws TimeoutException.
   */
  @Override
  public void waitOnBarrier() throws TimeoutException {

    boolean allArrived = incrementBarrierDAI(0, ControllerContext.maxWaitTimeOnBarrier(config));
    if (!allArrived) {

      // clear the value to zero, so that new barrier can be used
      try {
        daiForBarrier.forceSet(0);
      } catch (Exception e) {
        LOG.severe("DistributedAtomicInteger value can not be set to zero.");
      }

      throw new TimeoutException("All workers have not arrived at the barrier on the time limit: "
          + ControllerContext.maxWaitTimeOnBarrier(config) + "ms.");
    }
  }


  /**
   * close all local entities.
   */
  public void close() {
    try {
      super.close();
      workerZNode.close();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when closing", e);
    }
  }

}
