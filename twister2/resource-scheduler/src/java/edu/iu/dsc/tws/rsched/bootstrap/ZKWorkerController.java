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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
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
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.NodeInfo;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;

/**
 * gets unique workerID's for each client by using DistributedAtomicInteger
 * gets the current list of all workers from PathChildrenCache
 * it does not keep the list of worker nodes, it is already kept in PathChildrenCache
 * If a worker joins after failure, it gets its previous ID
 * There is no gap in id sequence.
 * All worker names and IDs are kept in the job node as data
 * Each worker appends its worker name-ID pair to this list when they join
 * They append to this list in synchronized fashion by acquiring a distributed lock:
 *   InterProcessMutex
 *
 * we use a barrier to make all workers wait until the last worker arrives at the barrier point
 * we count the number of waiting workers by using a DistributedAtomicInteger
 */

public class ZKWorkerController implements IWorkerController {
  public static final Logger LOG = Logger.getLogger(ZKWorkerController.class.getName());

  // hostname and port number of this worker
  private InetAddress workerIP;
  private int workerPort;

  // WorkerNetworkInfo object for this worker
  private WorkerNetworkInfo workerNetworkInfo;

  // number of workers in this job
  private int numberOfWorkers;

  // name of this job
  private String jobName;

  // NodeInfo object for this worker
  private NodeInfo nodeInfo;

  // the client to connect to ZK server
  private CuratorFramework client;

  // the path, znode and children cache objects for this job
  private String jobPath;
  private PersistentNode jobZNode;
  private PathChildrenCache childrenCache;

  // DistributedAtomicInteger for workerID generation
  private DistributedAtomicInteger daiForWorkerID;

  // variables related to the barrier
  private DistributedAtomicInteger daiForBarrier;
  private DistributedBarrier barrier;

  // config object
  private Config config;

  public ZKWorkerController(Config config,
                            String jobName,
                            String workerIpAndPort,
                            int numberOfWorkers,
                            NodeInfo nodeInfo) {
    this.config = config;
    this.jobName = jobName;
    this.numberOfWorkers = numberOfWorkers;
    this.nodeInfo = nodeInfo;
    this.jobPath = ZKUtil.constructJobPath(config, jobName);

    try {
      // convert hostname to IP before assigning
      String[] fields = workerIpAndPort.split(":");
      this.workerIP = InetAddress.getByName(fields[0]);
      this.workerPort = Integer.parseInt(fields[1]);
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Can not convert the given string to IP: " + workerIpAndPort, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * connect to the server
   * get a workerID for this worker
   * append this worker info to the body of job znode
   * create an ephemeral znode for this client
   * @return
   */
  public boolean initialize() {

    try {
      String zkServerAddresses = ZKContext.zooKeeperServerAddresses(config);
      client = CuratorFrameworkFactory.newClient(zkServerAddresses,
          new ExponentialBackoffRetry(1000, 3));
      client.start();

      String barrierPath = ZKUtil.constructBarrierPath(config, jobName);
      barrier = new DistributedBarrier(client, barrierPath);

      String daiPathForWorkerID = ZKUtil.constructDaiPathForWorkerID(config, jobName);
      daiForWorkerID = new DistributedAtomicInteger(client,
          daiPathForWorkerID, new ExponentialBackoffRetry(1000, 3));

      String daiPathForBarrier = ZKUtil.constructDaiPathForBarrier(config, jobName);
      daiForBarrier = new DistributedAtomicInteger(client,
          daiPathForBarrier, new ExponentialBackoffRetry(1000, 3));

      // check whether the job node exist, if not,
      // it means, this worker is the first worker to join
      // get a workerID, create the jobZnode, append worker info
      if (client.checkExists().forPath(jobPath) == null) {
        int workerID = createWorkerID();
        workerNetworkInfo = new WorkerNetworkInfo(workerIP, workerPort, workerID, nodeInfo);
        createWorkerZnode();
        appendWorkerInfo();

        // if the job node exists, it is not the first worker
        // check whether this worker joined the job before
        // whether it is coming from a failure
      } else {
        List<WorkerNetworkInfo> workers = parseJobZNode();
        workerNetworkInfo = getIfExists(workers);

        // this worker is coming from a failure,
        // use the workerNetworkInfo from job znode, construct worker znode only
        if (workerNetworkInfo != null) {
          createWorkerZnode();
          LOG.warning("Worker is coming from a failure. It is using the previous job znode data: "
              + workerNetworkInfo);

        // it has not joined before,
        // create workerID, append its info to the jobZnode
        } else {
          int workerID = createWorkerID();
          workerNetworkInfo = new WorkerNetworkInfo(workerIP, workerPort, workerID, nodeInfo);
          createWorkerZnode();
          appendWorkerInfo();
        }
      }

      // We childrenCache children data for parent path.
      // So we will listen for all workers in the job
      childrenCache = new PathChildrenCache(client, jobPath, true);
      childrenCache.start();

      LOG.info("This worker: " + workerNetworkInfo + " initialized successfully.");

      return true;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception when initializing ZKWorkerController", e);
      return false;
    }
  }

  @Override
  public WorkerNetworkInfo getWorkerNetworkInfo() {
    return workerNetworkInfo;
  }

  @Override
  public WorkerNetworkInfo getWorkerNetworkInfoForID(int id) {
    List<WorkerNetworkInfo> workerList = getWorkerList();
    for (WorkerNetworkInfo info: workerList) {
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
   * create worker ID for this worker by increasing shared DistributedAtomicInteger
   * re-try until it is created.
   */
  private int createWorkerID() {
    try {
      AtomicValue<Integer> incremented = daiForWorkerID.increment();
      if (incremented.succeeded()) {
        int workerID = incremented.preValue();
        LOG.fine("Unique WorkerID generated: " + workerID);
        return workerID;
      } else {
        createWorkerID();
      }
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Failed to generate a unique workerID. Will try again ...", e);
      createWorkerID();
    }

    return -1;
  }

  /**
   * create the znode for this worker
   */
  private void createWorkerZnode() {
    try {
      String thisNodePath =
          ZKUtil.constructWorkerPath(jobPath, workerNetworkInfo.getWorkerIpAndPort());
      String encodedWorkerNetworkInfo =
          WorkerNetworkInfo.encodeWorkerNetworkInfo(workerNetworkInfo);

      jobZNode = ZKUtil.createPersistentEphemeralZnode(
          client, thisNodePath, encodedWorkerNetworkInfo.getBytes());

      jobZNode.start();
      jobZNode.waitForInitialCreate(10000, TimeUnit.MILLISECONDS);
      String fullZnodePath = jobZNode.getActualPath();
      LOG.fine("An ephemeral znode is created for this worker: " + fullZnodePath);
    } catch (Exception e) {
      throw new RuntimeException("Could not create znode for the worker: " + workerNetworkInfo, e);
    }
  }

  /**
   * append this worker info to the data of parent znode
   * appends the data in synchronized block
   * it first acquires a lock, updates the data and release the lock
   */
  private void appendWorkerInfo() {

    String lockPath = ZKUtil.constructJobLockPath(config, jobName);
    InterProcessMutex lock = new InterProcessMutex(client, lockPath);
    try {
      lock.acquire();
      byte[] parentData = client.getData().forPath(jobPath);
      String parentStr = new String(parentData);
      String updatedParentStr = parentStr
          + "\n" + WorkerNetworkInfo.encodeWorkerNetworkInfo(workerNetworkInfo);
      client.setData().forPath(jobPath, updatedParentStr.getBytes());
      lock.release();
      LOG.info("Updated job znode content: " + updatedParentStr);
    } catch (Exception e) {
      throw new RuntimeException("Could not update the job znode content for the worker: "
          + workerNetworkInfo, e);
    }
  }

  /**
   * Print all given workers
   */
  public void printWorkers(List<WorkerNetworkInfo> workers) {

    StringBuffer logBuffer = new StringBuffer();
    logBuffer.append("Number of workers in the job: " + workers.size() + "\n");

    for (WorkerNetworkInfo worker: workers) {
      logBuffer.append(worker.toString() + "\n");
    }

    LOG.info(logBuffer.toString());
  }

  /**
   * Get current list of workers from local children cache
   * This list does not have the workers that have already left
   */
  public List<WorkerNetworkInfo> getCurrentWorkers() {

    List<WorkerNetworkInfo> workers = new ArrayList<WorkerNetworkInfo>();
    for (ChildData child: childrenCache.getCurrentData()) {
      String childContent = new String(child.getData());
      WorkerNetworkInfo wnInfo = WorkerNetworkInfo.decodeWorkerNetworkInfo(childContent);
      workers.add(wnInfo);
    }
    return workers;
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
  public List<WorkerNetworkInfo> getWorkerList() {

    return parseJobZNode();
  }

  /**
   * parse the job znode content
   * construct WorkerNetworkInfo objects
   * return them as a List
   * @return
   */
  private List<WorkerNetworkInfo> parseJobZNode() {
    byte[] jobZnodeData = null;
    try {
      jobZnodeData = client.getData().forPath(jobPath);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not get the job node data", e);
      return null;
    }

    List<WorkerNetworkInfo> workers = new ArrayList<WorkerNetworkInfo>();
    String jobZnodeStr = new String(jobZnodeData);
    StringTokenizer st = new StringTokenizer(jobZnodeStr, "\n");
    while (st.hasMoreTokens()) {
      String token = st.nextToken();
      WorkerNetworkInfo worker = WorkerNetworkInfo.decodeWorkerNetworkInfo(token);
      if (worker != null) {
        workers.add(worker);
      }
    }

    return workers;
  }

  /**
   * get the WorkerNetworkInfo object for this worker if it exists in the given list
   * @param workers
   * @return
   */
  private WorkerNetworkInfo getIfExists(List<WorkerNetworkInfo> workers) {
    String workerIpAndPort = workerIP.getHostAddress() + ":" + workerPort;
    for (WorkerNetworkInfo worker: workers) {
      if (workerIpAndPort.equalsIgnoreCase(worker.getWorkerIpAndPort())) {
        return worker;
      }
    }

    return null;
  }

  /**
   * count the number of all joined workers
   * count the workers based on their data availability on this worker
   * this count also includes the workers that have already completed
   */
  private int countNumberOfJoinedWorkers() {
    byte[] parentData = null;
    try {
      parentData = client.getData().forPath(jobPath);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not get the job node data", e);
      return -1;
    }

    String parentStr = new String(parentData);
    StringTokenizer st = new StringTokenizer(parentStr, "\n");
    return st.countTokens();
  }

  /**
   * wait to make sure that the number of workers reached the total number of workers in the job
   * return the all joined workers in the job including the ones that have already left
   * return null if timeLimit is reached or en exception is thrown while waiting
   */
  @Override
  public List<WorkerNetworkInfo> waitForAllWorkersToJoin(long timeLimitMilliSec) {

    long duration = 0;
    while (duration < timeLimitMilliSec) {
      if (countNumberOfJoinedWorkers() < numberOfWorkers) {
        try {
          Thread.sleep(50);
          duration += 50;
        } catch (InterruptedException e) {
          LOG.fine("Thread sleep interrupted. Will try again ...");
        }
      } else {
        return getWorkerList();
      }
    }

    LOG.warning("Waited for all workers to join, but timeLimit has been reached.");
    return null;
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
   * @param timeLimitMilliSec
   * @return
   */
  @Override
  public boolean waitOnBarrier(long timeLimitMilliSec) {

    return incrementBarrierDAI(0, timeLimitMilliSec);
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
        jobZNode.close();
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

