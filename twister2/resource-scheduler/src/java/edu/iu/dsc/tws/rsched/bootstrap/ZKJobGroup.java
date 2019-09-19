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

package edu.iu.dsc.tws.rsched.bootstrap;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.ControllerContext;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;

/**
 * we assume each worker is assigned a unique ID outside of this class
 * If a worker joins with an ID that already exists in the group,
 * we assume that the worker is coming from failure. It is the same worker.
 * It is very important that there is no worker ID collusion among different workers in the same job
 *
 * We create persistent znode for the job
 * We create an ephemeral znode for each worker under the job znode
 *
 * We keep the WorkerInfo objects of all workers in the body of job znode
 * We encode WorkerInfo objects as byte arrays.
 * Before each WorkerInfo byte array, we put the length of the byte array as 4 bytes.
 * So WorkerInfo objects are decoded by using their lengths
 *
 * Even when a worker fails, and its znode is deleted from ZooKeeper,
 * Its workerInfo remains in the job Znode.
 * When the worker comes back from a failure,
 * its WorkerInfo is updated in the job znode.
 *
 * We do not keep the list of workers locally.
 * We get them from job znode when requested.
 *
 * We modify job znode body in a synchronized block by acquiring a distributed lock:
 *   InterProcessMutex
 *
 * we use a barrier to make all workers wait until the last worker arrives at the barrier point
 * we count the number of waiting workers by using a DistributedAtomicInteger
 *
 * Problems:
 *  When a worker joins, we first create worker znode,
 *  then we add its WorkerInfo to the body of job znode.
 *  So, there is a delay between these two actions.
 *  A worker znode may have been created but its workerInfo may not be added to the job znode
 *  Other workers may get worker join event but, when they read job znode,
 *  they may not find workerInfo there
 */

public class ZKJobGroup implements IWorkerController {
  public static final Logger LOG = Logger.getLogger(ZKJobGroup.class.getName());

  // WorkerInfo object for this worker
  private WorkerInfo workerInfo;

  // number of workers in this job
  private int numberOfWorkers;

  // name of this job
  private String jobName;

  // the client to connect to ZK server
  private CuratorFramework client;

  // the path, znode and children cache objects for this job
  private String jobPath;

  // persistent ephemeral znode for this worker
  private PersistentNode workerZNode;

  // children cache for job znode
  private PathChildrenCache childrenCache;

  // variables related to the barrier
  private DistributedAtomicInteger daiForBarrier;
  private DistributedBarrier barrier;

  // config object
  private Config config;
  private String rootPath;

  // a flag to show whether all workers joined the job
  // Initially it is false. It becomes true when all workers joined the job.
  private boolean allJoined = false;

  // synchronization object for waiting all workers to join the job
  private Object waitObject = new Object();

  public ZKJobGroup(Config config,
                    String jobName,
                    int numberOfWorkers,
                    WorkerInfo workerInfo) {
    this.config = config;
    this.rootPath = ZKContext.rootNode(config);

    this.jobName = jobName;
    this.numberOfWorkers = numberOfWorkers;
    this.jobPath = ZKUtil.constructJobPath(rootPath, jobName);
    this.workerInfo = workerInfo;
  }

  /**
   * connect to the server
   * append this worker info to the body of job znode
   * create an ephemeral znode for this worker
   * worker znode body has the state of this worker
   * it will be updated as the status of worker changes from STARTING, RUNNING, COMPLETED
   * @return
   */
  public boolean initialize() throws Exception {

    try {
      String zkServerAddresses = ZKContext.zooKeeperServerAddresses(config);
      client = CuratorFrameworkFactory.newClient(zkServerAddresses,
          new ExponentialBackoffRetry(1000, 3));
      client.start();

      String barrierPath = ZKUtil.constructBarrierPath(rootPath, jobName);
      barrier = new DistributedBarrier(client, barrierPath);

      String daiPathForBarrier = ZKUtil.constructDaiPathForBarrier(rootPath, jobName);
      daiForBarrier = new DistributedAtomicInteger(client,
          daiPathForBarrier, new ExponentialBackoffRetry(1000, 3));

      // if this is the first worker to join the group
      // first create znode, then update job znode body
      // otherwise, first update job znode body, then create the worker znode
      if (client.checkExists().forPath(jobPath) == null) {
        createWorkerZnode();
        updateJobZnodeBody();
      } else {
        updateJobZnodeBody();
        createWorkerZnode();
      }

      // We childrenCache children data for parent path.
      // So we will listen for all workers in the job
      childrenCache = new PathChildrenCache(client, jobPath, true);
      addListener(childrenCache);
      childrenCache.start();

      LOG.info("This worker: " + workerInfo.getWorkerID() + " initialized successfully.");
      return true;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when initializing ZKJobGroup", e);
      throw e;
    }
  }

  @Override
  public WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  @Override
  public WorkerInfo getWorkerInfoForID(int id) {
    List<WorkerInfo> workerList = getJoinedWorkers();
    for (WorkerInfo info: workerList) {
      if (info.getWorkerID() == id) {
        return info;
      }
    }

    return null;
  }

  @Override
  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  /**
   * create the znode for this worker
   */
  private void createWorkerZnode() {
    try {
      String workerPath = ZKUtil.constructWorkerPath(jobPath, "w-" + workerInfo.getWorkerID());

      // put worker state into znode body
      byte[] workerZnodeBody =
          (JobMasterAPI.WorkerState.STARTING_VALUE + "").getBytes(StandardCharsets.UTF_8);

      workerZNode =
          ZKUtil.createPersistentEphemeralZnode(client, workerPath, workerZnodeBody);
      workerZNode.start();
      workerZNode.waitForInitialCreate(10000, TimeUnit.MILLISECONDS);

      String fullWorkerPath = workerZNode.getActualPath();
      LOG.fine("An ephemeral znode is created for this worker: " + fullWorkerPath);

    } catch (Exception e) {
      LOG.log(Level.SEVERE,
          "Could not create znode for the worker: " + workerInfo.getWorkerID(), e);
      throw new RuntimeException("Could not create znode for the worker: " + workerInfo, e);
    }
  }

  /**
   * check whether this workerInfo exists in the jobZnode body
   * if it exists, remove it. It means, it is coming from failure.
   * we append new WorkerInfo object to the job znode body.
   *
   * modify job znode data in synchronized block
   * other workers may update simultaneously
   * it first acquires a lock, updates the data and release the lock
   */
  private void updateJobZnodeBody() {

    String lockPath = ZKUtil.constructJobLockPath(rootPath, jobName);
    InterProcessMutex lock = new InterProcessMutex(client, lockPath);
    try {
      lock.acquire();
      byte[] parentData = client.getData().forPath(jobPath);
      List<WorkerInfo> existingWorkers = ZKUtil.decodeWorkerInfos(parentData);

      // job znode body byte array
      byte[] allBytes;

      // if current worker exists in znode, delete that
      if (deleteIfExists(existingWorkers)) {
        existingWorkers.add(workerInfo);
        allBytes = ZKUtil.encodeWorkerInfos(existingWorkers);

      // if current worker does not exist, append the current worker to the body
      } else {
        byte[] encodedWorkerInfoBytes = ZKUtil.encodeWorkerInfo(workerInfo);
        allBytes = ZKUtil.addTwoByteArrays(parentData, encodedWorkerInfoBytes);
      }

      client.setData().forPath(jobPath, allBytes);
      lock.release();
      LOG.info("Added own WorkerInfo and updated job znode content.");
    } catch (Exception e) {
      throw new RuntimeException("Could not update the job znode content for the worker: "
          + workerInfo.getWorkerID(), e);
    }
  }

  /**
   * Get current list of workers from local children cache
   * This list does not have the workers that have failed or already left
   */
  public List<WorkerInfo> getCurrentWorkers() {

    List<WorkerInfo> workers = parseJobZNode();
    List<WorkerInfo> currentWorkers = new ArrayList<>();

    for (ChildData child: childrenCache.getCurrentData()) {
      int id = getWorkerIDFromPath(child.getPath());
      WorkerInfo worker = getWorkerInfoForID(workers, id);
      if (worker != null) {
        currentWorkers.add(worker);
      }

    }
    return currentWorkers;
  }

  private WorkerInfo getWorkerInfoForID(List<WorkerInfo> workers, int id) {
    for (WorkerInfo info: workers) {
      if (info.getWorkerID() == id) {
        return info;
      }
    }

    return null;
  }

  /**
   * get number of current workers in the job as seen from this worker
   */
  public int getNumberOfCurrentWorkers() {
    return childrenCache.getCurrentData().size();
  }

  /**
   * Get all joined workers including the ones that have already completed and left
   */
  @Override
  public List<WorkerInfo> getJoinedWorkers() {

    return parseJobZNode();
  }

  /**
   * parse the job znode content
   * construct WorkerInfo objects
   * return them as a List
   * @return
   */
  private List<WorkerInfo> parseJobZNode() {
    try {
      byte[] jobZnodeData = client.getData().forPath(jobPath);
      return ZKUtil.decodeWorkerInfos(jobZnodeData);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not get the job node data", e);
      return null;
    }
  }

  /**
   * if there is a WorkerInfo object in the given list with the same workerID,
   * delete it and return true
   * otherwise, do nothing and return false
   * @param workers
   * @return
   */
  private boolean deleteIfExists(List<WorkerInfo> workers) {
    for (WorkerInfo worker: workers) {
      if (workerInfo.getWorkerID() == worker.getWorkerID()) {
        workers.remove(worker);
        return true;
      }
    }

    return false;
  }

  /**
   * count the number of WorkerInfo objects encoded in the job znode
   *
   * count the workers based on their data availability on this worker
   * this count also includes the workers that have already completed
   */
  private int countNumberOfJoinedWorkers() {

    byte[] parentData;
    try {
      parentData = client.getData().forPath(jobPath);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not get the job node data", e);
      return -1;
    }

    int lengthIndex = 0;
    int counter = 0;
    while (lengthIndex < parentData.length) {
      int length = ZKUtil.intFromBytes(parentData, lengthIndex);
      lengthIndex += 4 + length;
      counter++;
    }

    return counter;
  }

  /**
   * wait to make sure that the number of workers reached the total number of workers in the job
   * return all joined workers in the job including the ones that have already left
   * return null if the timeLimit is reached or an exception is thrown while waiting
   */
  @Override
  public List<WorkerInfo> getAllWorkers() throws TimeoutException {

    // if all workers already joined, return the workers list
    if (allJoined) {
      return getJoinedWorkers();
    }

    // wait until all workers joined or time limit is reached
    long timeLimit = ControllerContext.maxWaitTimeForAllToJoin(config);
    long startTime = System.currentTimeMillis();

    long delay = 0;
    while (delay < timeLimit) {
      synchronized (waitObject) {
        try {
          waitObject.wait(timeLimit - delay);

          // proceeding with notification or timeout
          if (allJoined) {
            return getJoinedWorkers();
          } else {
            throw new TimeoutException("Not all workers joined the job on the given time limit: "
                + timeLimit + "ms.");
          }

        } catch (InterruptedException e) {
          delay = System.currentTimeMillis() - startTime;
        }
      }
    }

    if (allJoined) {
      return getJoinedWorkers();
    } else {
      throw new TimeoutException("Not all workers joined the job on the given time limit: "
          + timeLimit + "ms.");
    }
  }

  /**
   * first 40 characters are random GUID prefix
   * remove them and return the remaining chars
   * @return
   */
  private String getZnodeName(String znodeName) {
    if (znodeName == null || znodeName.length() < 40) {
      return null;
    }

    String workerName = znodeName.substring(40);
    return workerName;
  }

  /**
   * WorkerID is at the end of workerPath
   * The string "w-" proceeds the workerID
   * @return
   */
  private int getWorkerIDFromPath(String workerPath) {
    String workerIDStr = workerPath.substring(workerPath.lastIndexOf("w-") + 2);
    return Integer.parseInt(workerIDStr);
  }

  /**
   * try to increment the daiForBarrier
   * try 10 times if fails
   * @param tryCount
   * @return
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
        if (incremented.postValue() % numberOfWorkers == 0) {
          barrier.removeBarrier();
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
   * that have reached to the barrier point
   *
   * Last worker to call this method and to increase the DistributedAtomicInteger,
   * removes the barrier and lets all previous waiting workers be released
   *
   * other workers to call this method and to increase the DistributedAtomicInteger,
   * enables the barrier by calling setBarrier method and wait
   *
   * it is enough to call setBarrier method by only the first worker,
   * however, it does not harm calling by many workers
   *
   * if we let only the first worker to set the barrier with setBarrier method,
   * then, the second worker may call this method after the dai is increased
   * but before the setBarrier method is called. To prevent this,
   * we may need to use a distributed InterProcessMutex.
   * So, instead of using a distributed InterProcessMutex, we call this method many times
   *
   * DistributedAtomicInteger always increases.
   * We check whether it is a multiple of numberOfWorkers in a job
   * If so, all workers have reached the barrier
   *
   * this method may be called many times during a computation
   *
   * return true if all workers have reached the barrier and they are all released
   * if timeout is reached, return false
   * @return
   */
  @Override
  public void waitOnBarrier() throws TimeoutException {

    boolean allArrived = incrementBarrierDAI(0, ControllerContext.maxWaitTimeOnBarrier(config));
    if (!allArrived) {
      throw new TimeoutException("All workers have not arrived at the barrier on the time limit: "
          + ControllerContext.maxWaitTimeOnBarrier(config) + "ms.");
    }
  }

  private void addListener(PathChildrenCache cache) {
    PathChildrenCacheListener listener = new PathChildrenCacheListener() {

      public void childEvent(CuratorFramework clientOfEvent, PathChildrenCacheEvent event)
          throws Exception {

        switch (event.getType()) {
          case CHILD_ADDED:
            if (numberOfWorkers == cache.getCurrentData().size()) {
              allJoined = true;
              synchronized (waitObject) {
                waitObject.notify();
              }
            }
            System.out.println("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
            break;

          case CHILD_UPDATED:
            System.out.println("Node Changed: "
                + ZKPaths.getNodeFromPath(event.getData().getPath()));
            break;

          case CHILD_REMOVED:
            System.out.println("Node removed: "
                + ZKPaths.getNodeFromPath(event.getData().getPath()));
            break;

          default:
            // nothing to do
        }
      }
    };
    cache.getListenable().addListener(listener);
  }



  /**
   * close the children cache
   * close persistent node for this worker
   * close the connection
   *
   * if this is the last worker to complete, delete all relevant znode for this job
   */
  public void close() {
    if (client != null) {
      try {
        int noOfChildren =  childrenCache.getCurrentData().size();
        workerZNode.close();
        CloseableUtils.closeQuietly(childrenCache);
        // if this is the last worker, delete znodes for the job
        if (noOfChildren == 1) {
          LOG.log(Level.INFO, "This is the last worker to finish. Deleting the job znodes.");
          ZKUtil.deleteJobZNodes(config, client, jobName);
        }
        CloseableUtils.closeQuietly(client);
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Exception when closing", e);
      }
    }
  }

}

