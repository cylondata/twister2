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

import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.api.exceptions.Twister2Exception;

/**
 * When a worker starts, it needs to know whether it is starting for the first time or
 * it is restarting from failure.
 * <p>
 * We create a persistent znode for each worker in the job.
 * If this znode exists for a worker, it means that this worker has started before.
 * When workers scaled down, we must delete the znodes of killed worker.
 * This is handled by the scaler.
 */
public final class ZKRestartCheck {
  public static final Logger LOG = Logger.getLogger(ZKRestartCheck.class.getName());

  private ZKRestartCheck() { }

  /**
   * Create job znode for worker and job master restart checking
   * Assumes that there is no znode exists in the ZooKeeper
   * This method should be called by the submitting client
   */
  public static void createJobZNode(CuratorFramework client, String rootPath, String jobName)
      throws Exception {

    String checkPath = ZKUtils.constructJobRestartCheckPath(rootPath, jobName);

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(checkPath);

      LOG.info("RestartCheckPath created: " + checkPath);

    } catch (Exception e) {
      throw new Exception("RestartCheckPath can not be created for the path: " + checkPath, e);
    }
  }

  /**
   * If the worker is starting for the first time,
   * This method returns false
   * It creates a znode for this worker on ZooKeeper server
   * Subsequent calls to this method returns true
   * Each worker must call this method exactly once when they start
   */
  public static boolean isWorkerRestarting(CuratorFramework client,
                                           String rootPath,
                                           String jobName,
                                           int workerID) throws Exception {

    return isEntityRestarting(client, rootPath, jobName, workerID + "");
  }

  /**
   * If the job master is starting for the first time,
   * This method returns false
   * It creates a znode for the job master on ZooKeeper server
   * Subsequent calls to this method returns true
   * Job Master must call this method exactly once when they start
   */
  public static boolean isJobMasterRestarting(CuratorFramework client,
                                           String rootPath,
                                           String jobName) throws Exception {

    return isEntityRestarting(client, rootPath, jobName, "jm");
  }

  /**
   * If the entity (worker or job master) is starting for the first time,
   * This method returns false
   * It creates a znode for this worker on ZooKeeper server
   * Subsequent calls to this method returns true
   * Each worker must call this method exactly once when they start or restart
   */
  public static boolean isEntityRestarting(CuratorFramework client,
                                           String rootPath,
                                           String jobName,
                                           String entityID) throws Exception {

    String jobCheckPath = ZKUtils.constructJobRestartCheckPath(rootPath, jobName);
    String entityCheckPath = ZKUtils.constructRestartCheckPath(jobCheckPath, entityID);

    if (client.checkExists().forPath(entityCheckPath) != null) {
      LOG.warning("RestartCheckPath exists: " + entityCheckPath);
      return true;
    }

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(entityCheckPath);

      LOG.info("RestartCheckPath created: " + entityCheckPath);
      return false;

    } catch (Exception e) {
      throw new Exception("RestartCheckPath can not be created: " + entityCheckPath, e);
    }
  }

  /**
   * When a job is scaled down, we must delete the znodes of killed workers.
   * minID inclusive, maxID exclusive
   */
  public static void removeScaledDownZNodes(CuratorFramework client,
                                            String rootPath,
                                            String jobName,
                                            int minID,
                                            int maxID) throws Twister2Exception {

    String checkPath = ZKUtils.constructJobRestartCheckPath(rootPath, jobName);

    for (int workerID = minID; workerID < maxID; workerID++) {
      String workerCheckPath = ZKUtils.constructWorkerRestartCheckPath(checkPath, workerID);

      try {
        // not sure whether we need to check the existence
        if (client.checkExists().forPath(workerCheckPath) != null) {

          client.delete().forPath(workerCheckPath);
          LOG.info("WorkerRestartCheckPath delete: " + workerCheckPath);

        }
      } catch (Exception e) {
        throw new Twister2Exception("WorkerRestartCheckPath cannot be deleted: " + workerCheckPath,
            e);
      }
    }
  }

}
