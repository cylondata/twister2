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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;

/**
 * this class provides methods to construct znode path names for jobs and workers
 * in addition, it provides methods to cleanup znodes on zookeeper server
 */
public final class ZKUtil {
  public static final Logger LOG = Logger.getLogger(ZKUtil.class.getName());

  private ZKUtil() {
  }

  /**
   * connect to ZooKeeper server
   * @param config
   * @return
   */
  public static CuratorFramework connectToServer(Config config) {

    String zkServer = ZKContext.zooKeeperServerAddresses(config);

    try {
      CuratorFramework client =
          CuratorFrameworkFactory.newClient(zkServer, new ExponentialBackoffRetry(1000, 3));
      client.start();

      LOG.log(Level.INFO, "Connected to ZooKeeper server: " + zkServer);
      return client;

    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not connect to ZooKeeper server" + zkServer, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * check whether there is an active job
   * if not, but there are znodes from previous sessions, those will be deleted
   * @param jobName
   * @return
   */
  public static boolean isThereAnActiveJob(String jobName, Config config) {
    try {
      CuratorFramework client = connectToServer(config);

      String jobPath = constructJobPath(config, jobName);

      // check whether the job node exists, if not, return false, nothing to do
      if (client.checkExists().forPath(jobPath) == null) {
        return false;

        // if the node exists but does not have any children, remove the job related znodes
      } else if (client.getChildren().forPath(jobPath).size() == 0) {
        deleteJobZNodes(config, client, jobName);
        client.close();

        return false;

        // if there are some children of job znode, it means there is an active job
        // don't delete, return true
      } else {
        return true;
      }

    } catch (Exception e) {
      LOG.log(Level.SEVERE, "", e);
      return false;
    }
  }

  /**
   * construct a job path from the given job name
   * @param jobName
   * @return
   */
  public static String constructJobPath(Config config, String jobName) {
    return ZKContext.rootNode(config) + "/" + jobName;
  }

  /**
   * construct a distributed atomic integer path for assigning worker ids
   * @param jobName
   * @return
   */
  public static String constructDaiPathForWorkerID(Config config, String jobName) {
    return ZKContext.rootNode(config) + "/" + jobName + "-dai-for-worker-id";
  }

  /**
   * construct a distributed atomic integer path for barrier
   * @param jobName
   * @return
   */
  public static String constructDaiPathForBarrier(Config config, String jobName) {
    return ZKContext.rootNode(config) + "/" + jobName + "-dai-for-barrier";
  }

  /**
   * construct a distributed barrier path
   * @param jobName
   * @return
   */
  public static String constructBarrierPath(Config config, String jobName) {
    return ZKContext.rootNode(config) + "/" + jobName + "-barrier";
  }

  /**
   * construct a job distributed lock path from the given job name
   * @param jobName
   * @return
   */
  public static String constructJobLockPath(Config config, String jobName) {
    return ZKContext.rootNode(config) + "/" + jobName + "-lock";
  }

  /**
   * construct a worker path from the given job path and worker network info
   * @return
   */
  public static String constructWorkerPath(String jobPath, String workerHostAndPort) {
    return jobPath + "/" + workerHostAndPort;
  }

  /**
   * construct a worker path from the given job path and worker network info
   * @return
   */
  public static String constructJobMasterPath(Config config) {
    return ZKContext.rootNode(config) + "/" + Context.jobName(config) + "-job-master";
  }

  /**
   * delete job related znode from previous sessions
   * @param jobName
   * @return
   */
  public static boolean deleteJobZNodes(Config config, CuratorFramework client, String jobName) {
    try {
      String jobPath = constructJobPath(config, jobName);
      if (client.checkExists().forPath(jobPath) != null) {
        client.delete().deletingChildrenIfNeeded().forPath(jobPath);
        LOG.log(Level.INFO, "Job Znode deleted from ZooKeeper: " + jobPath);
      } else {
        LOG.log(Level.INFO, "No job znode exists in ZooKeeper to delete for: " + jobPath);
      }

      // delete distributed atomic integer for workerID
      String daiPath = constructDaiPathForWorkerID(config, jobName);
      if (client.checkExists().forPath(daiPath) != null) {
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(daiPath);
        LOG.info("DistributedAtomicInteger for workerID deleted from ZooKeeper: " + daiPath);
      } else {
        LOG.info("DistributedAtomicInteger for workerID not deleted from ZooKeeper: " + daiPath);
      }

      // delete distributed atomic integer for barrier
      daiPath = constructDaiPathForBarrier(config, jobName);
      if (client.checkExists().forPath(daiPath) != null) {
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(daiPath);
        LOG.info("DistributedAtomicInteger for barrier deleted from ZooKeeper: " + daiPath);
      } else {
        LOG.info("DistributedAtomicInteger for workerID not deleted from ZooKeeper: " + daiPath);
      }

      // delete distributed lock znode
      String lockPath = constructJobLockPath(config, jobName);
      if (client.checkExists().forPath(lockPath) != null) {
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(lockPath);
        LOG.log(Level.INFO, "Distributed lock znode deleted from ZooKeeper: " + lockPath);
      } else {
        LOG.log(Level.INFO, "No distributed lock znode to delete from ZooKeeper: " + lockPath);
      }

      return true;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "", e);
      return false;
    }
  }

  /**
   * delete all znodes related to the given jobName
   * @param jobName
   * @return
   */
  public static boolean terminateJob(String jobName, Config config) {
    try {
      CuratorFramework client = connectToServer(config);
      boolean deleteResult = deleteJobZNodes(config, client, jobName);
      client.close();
      return deleteResult;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not delete job znodes", e);
      return false;
    }
  }

  /**
   * create a PersistentNode object in the given path
   * it is ephemeral and persistent
   * it will be deleted after the worker leaves or fails
   * it will be persistent for occasional network problems
   * @param path
   * @param payload
   * @return
   * @throws Exception
   */
  public static PersistentNode createPersistentEphemeralZnode(CuratorFramework client,
                                                              String path,
                                                              byte[] payload) {

    return new PersistentNode(client, CreateMode.EPHEMERAL, true, path, payload);
  }

  /**
   * create a PersistentNode object in the given path
   * it needs to be deleted explicitly, not ephemeral
   * it will be persistent for occasional network problems
   * @param path
   * @param payload
   * @return
   * @throws Exception
   */
  public static PersistentNode createPersistentZnode(CuratorFramework client,
                                                              String path,
                                                              byte[] payload) {

    return new PersistentNode(client, CreateMode.EPHEMERAL, false, path, payload);
  }


}
