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
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public final class ZKBarrierManager {
  public static final Logger LOG = Logger.getLogger(ZKBarrierManager.class.getName());

  private ZKBarrierManager() {
  }

  /**
   * create parent directory for ephemeral worker znodes
   */
  public static void createBarrierDir(CuratorFramework client, String rootPath, String jobName)
      throws Exception {

    String barrierDirPath = ZKUtils.barrierDir(rootPath, jobName);

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(barrierDirPath);

      LOG.info("Job BarrierStateDir created: " + barrierDirPath);

    } catch (Exception e) {
      throw new Exception("BarrierStateDir can not be created for the path: " + barrierDirPath, e);
    }
  }

  /**
   * create a worker znode at the barrier directory
   */
  public static void createWorkerZNode(CuratorFramework client,
                                  String rootPath,
                                  String jobName,
                                  int workerID) {
    String barrierPath = ZKUtils.barrierDir(rootPath, jobName);
    String workerPath = ZKUtils.workerPath(barrierPath, workerID);

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(workerPath);

      LOG.info("Worker Barrier Znode created: " + workerPath);

    } catch (Exception e) {
      throw new Twister2RuntimeException("Worker Barrier Znode can not be created for the path: "
          + workerPath, e);
    }
  }

  /**
   * create a worker znode at the barrier directory
   */
  public static void deleteWorkerZNode(CuratorFramework client,
                                       String rootPath,
                                       String jobName,
                                       int workerID) {
    String barrierPath = ZKUtils.barrierDir(rootPath, jobName);
    String workerPath = ZKUtils.workerPath(barrierPath, workerID);

    try {
      client
          .delete()
          .forPath(workerPath);

      LOG.info("Worker Barrier Znode deleted: " + workerPath);

    } catch (Exception e) {
      throw new Twister2RuntimeException("Worker Barrier Znode can not be deleted for the path: "
          + workerPath, e);
    }
  }

  /**
   * create a worker znode at the barrier directory
   */
  public static boolean existWorkerZNode(CuratorFramework client,
                                       String rootPath,
                                       String jobName,
                                       int workerID) {
    String barrierPath = ZKUtils.barrierDir(rootPath, jobName);
    String workerPath = ZKUtils.workerPath(barrierPath, workerID);

    try {
      return client.checkExists().forPath(workerPath) != null;
    } catch (Exception e) {
      throw new Twister2RuntimeException("Can not check existence of Worker Barrier Znode: "
          + workerPath, e);
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

    String barrierDir = ZKUtils.barrierDir(rootPath, jobName);

    for (int workerID = minID; workerID < maxID; workerID++) {
      String workerPath = ZKUtils.workerPath(barrierDir, workerID);

      try {
        // not sure whether we need to check the existence
        if (client.checkExists().forPath(workerPath) != null) {

          client.delete().forPath(workerPath);
          LOG.info("Worker Barrier Znode deleted: " + workerPath);
        }
      } catch (Exception e) {
        throw new Twister2Exception("Worker Barrier Znode cannot be deleted: " + workerPath, e);
      }
    }
  }

}
