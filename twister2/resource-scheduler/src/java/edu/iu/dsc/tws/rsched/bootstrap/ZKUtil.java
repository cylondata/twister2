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
import org.apache.curator.retry.ExponentialBackoffRetry;

import edu.iu.dsc.tws.common.config.Config;
//import static edu.iu.dsc.tws.rsched.bootstrap.ZKContext.ROOT_NODE;

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

    String zkServerAddress = ZKContext.zooKeeperServerIP(config);
    String zkServerPort = ZKContext.zooKeeperServerPort(config);
    String zkServer = zkServerAddress + ":" + zkServerPort;

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
      e.printStackTrace();
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
   * construct a job distributed atomic integer path from the given job name
   * @param jobName
   * @return
   */
  public static String constructJobDaiPath(Config config, String jobName) {
    return ZKContext.rootNode(config) + "/" + jobName + "-dai";
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
   * delete job related znode from previous sessions
   * @param jobName
   * @return
   */
  public static boolean deleteJobZNodes(Config config, CuratorFramework client, String jobName) {
    try {
      String jobPath = constructJobPath(config, jobName);
      if (client.checkExists().forPath(jobPath) != null) {
//        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(jobPath);
        client.delete().deletingChildrenIfNeeded().forPath(jobPath);
        LOG.log(Level.INFO, "Job Znode deleted from ZooKeeper: " + jobPath);
      } else {
        LOG.log(Level.INFO, "No job znode exists in ZooKeeper to delete for: " + jobPath);
      }

      // delete distributed atomic integer znode
      String daiPath = constructJobDaiPath(config, jobName);
      if (client.checkExists().forPath(daiPath) != null) {
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(daiPath);
        LOG.log(Level.INFO, "Distributed atomic integer znode deleted from ZooKeeper: " + daiPath);
      } else {
        LOG.log(Level.INFO, "No distributed atomic integer znode to delete from ZooKeeper: "
            + daiPath);
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
      e.printStackTrace();
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

}
