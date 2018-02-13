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
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.common.config.Config;

/**
 * gets unique workerID's for each client by using DistributedAtomicInteger
 * gets the current list of all workers from PathChildrenCache
 * it does not keep the list of worker nodes, it is already kept in PathChildrenCache
 * If a worker joins after failure, it gets its previous ID
 * There is no gap in id sequence.
 * All worker names and IDs are kept in job node as data
 * Each worker appends its worker name-ID pair to this list when they join
 * They append to this list in synchronized fashion by acquiring a distributed lock:
 *   InterProcessMutex
 * <p>
 */

public class ZKController {
  public static final Logger LOG = Logger.getLogger(ZKController.class.getName());

  private String zkAddress; // hostname and port number of ZooKeeper
  private String hostAndPort; // hostname and port number of this worker
  private WorkerInfo workerInfo;
  private CuratorFramework client;
  private String jobName;
  private String znodePath;
  private String jobPath;
  private String daiPath;
  private String lockPath;
  private PersistentNode thisNode;
  private PathChildrenCache childrenCache;
  private DistributedAtomicInteger dai;
  private Config config;

  public ZKController(Config config, String jobName, String hostAndPort) {
    this.config = config;
    this.hostAndPort = hostAndPort;
    this.jobName = jobName;
    this.jobPath = ZKUtil.constructJobPath(config, jobName);
    this.daiPath = ZKUtil.constructJobDaiPath(config, jobName);
    this.lockPath = ZKUtil.constructJobLockPath(config, jobName);
  }

  /**
   * connect to the server
   * get a workerID for this worker
   * create an ephemeral znode for this client
   * @return
   */
  public boolean initialize() {
    String zkServerAddress = ZKContext.zooKeeperServerIP(config);
    String zkServerPort = ZKContext.zooKeeperServerPort(config);
    zkAddress = zkServerAddress + ":" + zkServerPort;

    try {
      client = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(1000, 3));
      client.start();

      dai = new DistributedAtomicInteger(client, daiPath, new ExponentialBackoffRetry(1000, 3));

      // check whether the job node exist, if not, get a workerID
      // if the job node does not exists, it means that this is not rejoining
      if (client.checkExists().forPath(jobPath) == null) {
        int workerID = createWorkerID();
        workerInfo = new WorkerInfo(hostAndPort, workerID);
        createWorkerZnode();
        appendWorkerInfo();

        // if the parent exists, check whether this worker joined the job before
        // whether it is coming from a failure
      } else {
        byte[] parentData = client.getData().forPath(jobPath);
        String parentStr = new String(parentData);

        if (parentStr.indexOf(hostAndPort) < 0) {
          int workerID = createWorkerID();
          workerInfo = new WorkerInfo(hostAndPort, workerID);
          createWorkerZnode();
          appendWorkerInfo();

          // if this worker is coming from a failure, get the ID from the parent content
        } else {
          int workerID = getWorkerIDFromParentData(parentStr);
          workerInfo = new WorkerInfo(hostAndPort, workerID);
          createWorkerZnode();
        }
      }

      // We childrenCache children data for parent path.
      // So we will listen for all workers in the job
      childrenCache = new PathChildrenCache(client, jobPath, true);
      childrenCache.start();

      LOG.log(Level.INFO, "This worker: " + workerInfo + " initialized successfully.");

      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  public WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  /**
   * create worker ID for this worker by increasing shared DistributedAtomicInteger
   */
  private int createWorkerID() {
    try {
      AtomicValue<Integer> incremented = dai.increment();
      if (incremented.succeeded()) {
        int workerID = incremented.preValue();
        LOG.log(Level.INFO, "Unique WorkerID generated: " + workerID);
        return workerID;
      } else {
        createWorkerID();
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to generate a unique workerID. Will try again ...", e);
      createWorkerID();
    }

    return -1;
  }

  /**
   * create the znode for this worker
   */
  private void createWorkerZnode() {
    try {
      String tempNodePath = jobPath + "/" + hostAndPort;
      thisNode = createPersistentZnode(tempNodePath, workerInfo.getWorkerIDAsBytes());
      thisNode.start();
      thisNode.waitForInitialCreate(10000, TimeUnit.MILLISECONDS);
      znodePath = thisNode.getActualPath();
      LOG.log(Level.INFO, "An ephemeral znode is created for this worker: " + znodePath);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not create znode for the worker: " + hostAndPort);
      e.printStackTrace();
    }
  }

  /**
   * append this worker info to the data of parent znode
   * appends the data in synchronized block
   * it first acquires a lock, updates the data and release the lock
   */
  private void appendWorkerInfo() {

    InterProcessMutex lock = new InterProcessMutex(client, lockPath);
    try {
      lock.acquire();
      byte[] parentData = client.getData().forPath(jobPath);
      String parentStr = new String(parentData);
      String updatedParentStr = parentStr + "\n" + workerInfo.getWorkerInfoAsString();
      client.setData().forPath(jobPath, updatedParentStr.getBytes());
      lock.release();
      LOG.log(Level.INFO, "Updated job znode content: " + updatedParentStr);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not update job znode content for worker: " + hostAndPort);
      throw new RuntimeException(e);
    }
  }

  /**
   * parse the data of parent znode and retrieve the worker ID for this worker
   * @param parentStr
   */
  private int getWorkerIDFromParentData(String parentStr) {
    int workerID = WorkerInfo.getWorkerIDByParsing(parentStr, hostAndPort);
    LOG.log(Level.INFO, "Using workerID from previous session: " + workerInfo.getWorkerID());
    return workerID;
  }


  /**
   * create an Ephemeral Sequential znode with protection
   * ephemeral: it will be deleted once the client disconnects
   * with protection: if the client sends create request and can not receive reply,
   *   when it reconnects no new znode will be created, previous one will be used
   *   The name of the node that is created is prefixed with a GUID.
   *   GUID is used to determine prevously generated znode.
   * @param path
   * @param payload
   * @return
   * @throws Exception
   */
  public String createWorkerZnode(String path, byte[] payload) throws Exception {
    return client.create()
        .creatingParentsIfNeeded()
        .withProtection()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(path, payload);
  }

  /**
   * create a PersistentNode object for this worker in job znode
   * it is ephemeral and persistent
   * it will be deleted after the worker leaves or fails
   * it will be persistent for occasional network problems
   * @param path
   * @param payload
   * @return
   * @throws Exception
   */
  public PersistentNode createPersistentZnode(String path, byte[] payload) throws Exception {
    return new PersistentNode(client, CreateMode.EPHEMERAL, true, path, payload);
  }

  /**
   * Print all given workers
   */
  public void printWorkers(List<WorkerInfo> workers) {

    System.out.println("Number of workers in the job: " + workers.size());

    for (WorkerInfo worker: workers) {
      System.out.println(worker);
//      System.out.println(worker.getWorkerIP().getHostAddress()
//          + ":" + worker.getWorkerPort() + ":" + worker.getWorkerID());
    }
  }

  /**
   * Print current workers in the job as seen by this worker
   */
  public void printCurrentWorkers() {
    System.out.println("list of current workers in the job: ");
    List<WorkerInfo> workers = getCurrentWorkers();
    printWorkers(workers);
  }

  /**
   * Get current list of workers from local children cache
   */
  public List<WorkerInfo> getCurrentWorkers() {

    List<WorkerInfo> workers = new ArrayList<WorkerInfo>();
    for (ChildData child: childrenCache.getCurrentData()) {
      String fullPath = child.getPath();
      String znodeName = ZKPaths.getNodeFromPath(fullPath);
      String workerName = getZnodeName(znodeName);
      int id = WorkerInfo.getWorkerIDFromBytes(child.getData());
      workers.add(new WorkerInfo(workerName, id));
    }
    return workers;
  }

  /**
   * Get all joined workers including the ones finished
   */
  public List<WorkerInfo> getAllJoinedWorkers() {

    byte[] parentData = null;
    try {
      parentData = client.getData().forPath(jobPath);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not get job node data", e);
      return null;
    }

    List<WorkerInfo> workers = new ArrayList<WorkerInfo>();
    String parentStr = new String(parentData);
    StringTokenizer st = new StringTokenizer(parentStr, "\n");
    while (st.hasMoreTokens()) {
      String token = st.nextToken();
      WorkerInfo worker = WorkerInfo.getWorkerInfoFromString(token);
      workers.add(worker);
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
   * get the number of all joined workers to the job including the ones that already left
   * the worker info of some of those workers may not have arrived to this worker yet
   */
  public int getNumberOfJoinedWorkers() {
    try {
      return dai.get().preValue();
    } catch (Exception e) {
      e.printStackTrace();
      return -1;
    }
  }

  /**
   * count the number of all joined workers
   * count the workers based on their data availability on this worker
   */
  public int countNumberOfJoinedWorkers() {
    byte[] parentData = null;
    try {
      parentData = client.getData().forPath(jobPath);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not get job node data", e);
      return -1;
    }

    String parentStr = new String(parentData);
    StringTokenizer st = new StringTokenizer(parentStr, "\n");
    return st.countTokens();
  }

  /**
   * wait to make sure that the number of workers reached the total number of workers in the job
   * return the current set of workers in the job
   * some workers may have already left, so current worker list may be less than the total
   * return null if timeLimit is reached or en exception thrown while waiting
   */
  public List<WorkerInfo> waitForAllWorkersToJoin(int numberOfWorkers, long timeLimit) {

    long duration = 0;
    while (duration < timeLimit) {
      if (countNumberOfJoinedWorkers() < numberOfWorkers) {
        try {
          Thread.sleep(50);
          duration += 50;
        } catch (InterruptedException e) {
          LOG.log(Level.INFO, "Thread sleep interrupted. Will try again ...");
          e.printStackTrace();
        }
      } else {
        return getCurrentWorkers();
      }
    }

    LOG.log(Level.INFO, "Waited for all workers to join, but timeLimit has been reached");
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
        thisNode.close();
        CloseableUtils.closeQuietly(childrenCache);
        // if this is the last worker, delete znodes for the job
        if (noOfChildren == 1) {
          LOG.log(Level.INFO, "This is the last worker to finish. Deleting job znodes.");
          ZKUtil.deleteJobZNodes(config, client, jobName);
        }
        CloseableUtils.closeQuietly(client);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

}

