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

import com.google.common.primitives.Longs;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.api.exceptions.Twister2Exception;

public final class ZKBarrierManager {
  public static final Logger LOG = Logger.getLogger(ZKBarrierManager.class.getName());

  private ZKBarrierManager() {
  }

  /**
   * create parent directory for barrier
   */
  public static void createBarrierDir(CuratorFramework client, String barrierDirPath)
      throws Twister2Exception {

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(barrierDirPath);

      LOG.info("BarrierDirectory created: " + barrierDirPath);

    } catch (Exception e) {
      throw new Twister2Exception("BarrierDirectory can not be created for the path: "
          + barrierDirPath, e);
    }
  }

  /**
   * create parent directory for default barrier
   */
  public static void createDefaultBarrierDir(CuratorFramework client, String rootPath, String jobID)
      throws Twister2Exception {

    String barrierDirPath = ZKUtils.defaultBarrierDir(rootPath, jobID);
    createBarrierDir(client, barrierDirPath);
  }

  /**
   * create parent directory for init barrier
   */
  public static void createInitBarrierDir(CuratorFramework client, String rootPath, String jobID)
      throws Twister2Exception {

    String initDirPath = ZKUtils.initBarrierDir(rootPath, jobID);
    createBarrierDir(client, initDirPath);
  }

  /**
   * create a worker znode at the barrier directory
   */
  public static void createWorkerZNode(CuratorFramework client, String workerPath, long timeout)
      throws Twister2Exception {

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(workerPath, Longs.toByteArray(timeout));

      LOG.info("Worker Barrier Znode created: " + workerPath);

    } catch (Exception e) {
      throw new Twister2Exception("Worker Barrier Znode can not be created for the path: "
          + workerPath, e);
    }
  }

  /**
   * read a worker znode at the barrier directory
   */
  public static long readWorkerTimeout(CuratorFramework client, String workerPath)
      throws Twister2Exception {

    try {
      byte[] timeoutBytes = client.getData().forPath(workerPath);
      return Longs.fromByteArray(timeoutBytes);
    } catch (Exception e) {
      throw new Twister2Exception("Could not read worker barrier znode body: " + e.getMessage(), e);
    }
  }

  /**
   * create a worker znode at the default barrier directory
   */
  public static void createWorkerZNodeAtDefault(CuratorFramework client,
                                                String rootPath,
                                                String jobID,
                                                int workerID,
                                                long timeout) throws Twister2Exception {

    String barrierPath = ZKUtils.defaultBarrierDir(rootPath, jobID);
    String workerPath = ZKUtils.workerPath(barrierPath, workerID);
    createWorkerZNode(client, workerPath, timeout);
  }

  /**
   * create a worker znode at the init barrier directory
   */
  public static void createWorkerZNodeAtInit(CuratorFramework client,
                                             String rootPath,
                                             String jobID,
                                             int workerID,
                                             long timeout) throws Twister2Exception {

    String barrierPath = ZKUtils.initBarrierDir(rootPath, jobID);
    String workerPath = ZKUtils.workerPath(barrierPath, workerID);
    createWorkerZNode(client, workerPath, timeout);
  }

  /**
   * delete the worker znode at a barrier directory
   */
  public static void deleteWorkerZNode(CuratorFramework client, String workerPath)
      throws Twister2Exception {

    try {
      client
          .delete()
          .forPath(workerPath);

      LOG.info("Worker Barrier Znode deleted: " + workerPath);

    } catch (Exception e) {
      throw new Twister2Exception("Worker Barrier Znode can not be deleted for the path: "
          + workerPath, e);
    }
  }

  /**
   * delete the worker znode at the default barrier directory
   */
  public static void deleteWorkerZNodeFromDefault(CuratorFramework client,
                                                  String rootPath,
                                                  String jobID,
                                                  int workerID) throws Twister2Exception {
    String barrierPath = ZKUtils.defaultBarrierDir(rootPath, jobID);
    String workerPath = ZKUtils.workerPath(barrierPath, workerID);
    deleteWorkerZNode(client, workerPath);
  }

  /**
   * delete the worker znode at the init barrier directory
   */
  public static void deleteWorkerZNodeFromInit(CuratorFramework client,
                                               String rootPath,
                                               String jobID,
                                               int workerID) throws Twister2Exception {
    String barrierPath = ZKUtils.initBarrierDir(rootPath, jobID);
    String workerPath = ZKUtils.workerPath(barrierPath, workerID);
    deleteWorkerZNode(client, workerPath);
  }

  /**
   * check existence of a worker znode at a barrier directory
   */
  public static boolean existWorkerZNode(CuratorFramework client, String workerPath)
      throws Twister2Exception {

    try {
      return client.checkExists().forPath(workerPath) != null;
    } catch (Exception e) {
      throw new Twister2Exception("Can not check existence of Worker Barrier Znode: "
          + workerPath, e);
    }
  }

  /**
   * check existence of a worker znode at the default barrier directory
   */
  public static boolean existWorkerZNodeAtDefault(CuratorFramework client,
                                                  String rootPath,
                                                  String jobID,
                                                  int workerID) throws Twister2Exception {
    String barrierPath = ZKUtils.defaultBarrierDir(rootPath, jobID);
    String workerPath = ZKUtils.workerPath(barrierPath, workerID);
    return existWorkerZNode(client, workerPath);
  }

  /**
   * check existence of a worker znode at the init barrier directory
   */
  public static boolean existWorkerZNodeAtInit(CuratorFramework client,
                                               String rootPath,
                                               String jobID,
                                               int workerID) throws Twister2Exception {
    String barrierPath = ZKUtils.initBarrierDir(rootPath, jobID);
    String workerPath = ZKUtils.workerPath(barrierPath, workerID);
    return existWorkerZNode(client, workerPath);
  }

  /**
   * When a job is scaled down, we must delete the znodes of killed workers.
   * minID inclusive, maxID exclusive
   */
  public static void removeScaledDownZNodes(CuratorFramework client,
                                            String barrierDir,
                                            int minID,
                                            int maxID) throws Twister2Exception {

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

  /**
   * Remove barrier znodes from both default and init barriers
   * minID inclusive, maxID exclusive
   */
  public static void removeScaledDownZNodes(CuratorFramework client,
                                            String rootPath,
                                            String jobID,
                                            int minID,
                                            int maxID) throws Twister2Exception {

    String defaultBarrierDir = ZKUtils.defaultBarrierDir(rootPath, jobID);
    removeScaledDownZNodes(client, defaultBarrierDir, minID, maxID);

    String initBarrierDir = ZKUtils.initBarrierDir(rootPath, jobID);
    removeScaledDownZNodes(client, initBarrierDir, minID, maxID);
  }

  public static int getNumberOfWorkersAtBarrier(CuratorFramework client,
                                                String rootPath,
                                                String jobID) throws Twister2Exception {

    String barrierDir = ZKUtils.defaultBarrierDir(rootPath, jobID);

    try {
      int numberOfWorkersAt = client.getChildren().forPath(barrierDir).size();
      LOG.info("Number of workers at the barrier: " + numberOfWorkersAt);
      return numberOfWorkersAt;
    } catch (Exception e) {
      throw new Twister2Exception("Could not get children of barrier directory: "
          + barrierDir, e);
    }
  }

}
