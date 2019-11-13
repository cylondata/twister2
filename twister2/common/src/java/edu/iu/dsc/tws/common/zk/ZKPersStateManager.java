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

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.JobMasterState;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerState;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * This class has methods to keep persistent status of a job in ZooKeeper servers
 * Parent znode (directory) has the job object as its body
 * One persistent child znode is created for each worker with workerID as its name
 * Each worker znode has WorkerInfo and the last WorkerState as its body
 * A separate persistent znode is created for job master.
 * JM znode is not in workers directory. It is in the upper directory in the main job directory.
 * <p>
 * When a worker starts, it needs to know whether it is starting for the first time or
 * it is restarting from failure.
 * When starting, each worker checks whether there exists a znode for itself.
 * If the znode exists, it means that this worker has started before.
 * <p>
 * When the job is scaled down, we delete the znodes of killed workers.
 * This is handled by the scaler in Job Master.
 */
public final class ZKPersStateManager {
  public static final Logger LOG = Logger.getLogger(ZKPersStateManager.class.getName());

  private ZKPersStateManager() {
  }

  /**
   * Create job znode for persistent states
   * Assumes that there is no znode exists in the ZooKeeper
   * This method should be called by the submitting client
   */
  public static void createJobZNode(CuratorFramework client, String rootPath, JobAPI.Job job)
      throws Exception {

    String persStatePath = ZKUtils.constructWorkersPersDir(rootPath, job.getJobName());

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(persStatePath, job.toByteArray());

      LOG.info("Job PersStatePath created: " + persStatePath);

    } catch (Exception e) {
      throw new Exception("PersStatePath can not be created for the path: "
          + persStatePath, e);
    }
  }

  /**
   * Initialize worker persistent state at ZooKeeper server
   * If the worker is starting for the first time, returns false
   * If the worker is restarting, returns true
   *
   * A persistent znode is created/updated for this worker on ZooKeeper server
   * Each worker must call this method exactly once when they start
   */
  public static boolean initWorkerPersState(CuratorFramework client,
                                            String rootPath,
                                            String jobName,
                                            WorkerInfo workerInfo) throws Exception {

    String workersPersDir = ZKUtils.constructWorkersPersDir(rootPath, jobName);
    String workerPersPath = ZKUtils.constructWorkerPath(workersPersDir, workerInfo.getWorkerID());

    // if the worker znode exists,
    // update the body and return true
    if (client.checkExists().forPath(workerPersPath) != null) {
      LOG.warning("Worker PersStatePath exists: " + workerPersPath);
      WorkerWithState workerWithState = new WorkerWithState(workerInfo, WorkerState.RESTARTED);
      client.setData().forPath(workerPersPath, workerWithState.toByteArray());
      return true;
    }

    // if the worker znode does not exist,
    // create worker znode
    WorkerWithState workerWithState = new WorkerWithState(workerInfo, WorkerState.STARTED);
    client
        .create()
        .withMode(CreateMode.PERSISTENT)
        .forPath(workerPersPath, workerWithState.toByteArray());
    return false;
  }

  /**
   * Initialize job master persistent state at ZooKeeper server
   * If the job master is starting for the first time, return false
   * If the job master is restarting, return true
   *
   * A persistent znode is created/updated for the job master on ZooKeeper server
   * Job master must call this method exactly once when it starts
   */
  public static boolean initJobMasterPersState(CuratorFramework client,
                                               String rootPath,
                                               String jobName,
                                               String jmAddress) throws Exception {

    String jmPersPath = ZKUtils.constructJMPersPath(rootPath, jobName);

    // if the worker znode exists,
    // update the body and return true
    if (client.checkExists().forPath(jmPersPath) != null) {
      LOG.warning("JobMaster PersStatePath exists: " + jmPersPath);
      byte[] znodeBody =
          ZKUtils.encodeJobMasterZnode(jmAddress, JobMasterState.JM_RESTARTED.getNumber());
      client.setData().forPath(jmPersPath, znodeBody);
      return true;
    }

    // if the worker znode does not exist,
    // create worker znode
    byte[] znodeBody =
        ZKUtils.encodeJobMasterZnode(jmAddress, JobMasterState.JM_STARTED.getNumber());

    client
        .create()
        .withMode(CreateMode.PERSISTENT)
        .forPath(jmPersPath, znodeBody);
    return false;
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

    String checkPath = ZKUtils.constructWorkersPersDir(rootPath, jobName);

    for (int workerID = minID; workerID < maxID; workerID++) {
      String workerCheckPath = ZKUtils.constructWorkerPath(checkPath, workerID);

      try {
        // not sure whether we need to check the existence
        if (client.checkExists().forPath(workerCheckPath) != null) {

          client.delete().forPath(workerCheckPath);
          LOG.info("Worker PersStatePath deleted: " + workerCheckPath);
        }
      } catch (Exception e) {
        throw new Twister2Exception("Worker PersStatePath cannot be deleted: " + workerCheckPath,
            e);
      }
    }
  }

  public static boolean updateWorkerStatus(CuratorFramework client,
                                           String rootPath,
                                           String jobName,
                                           WorkerInfo workerInfo,
                                           WorkerState newStatus) {

    String workersPersDir = ZKUtils.constructWorkersPersDir(rootPath, jobName);
    String workerPersPath = ZKUtils.constructWorkerPath(workersPersDir, workerInfo.getWorkerID());
    WorkerWithState workerWithState = new WorkerWithState(workerInfo, newStatus);

    try {
      client.setData().forPath(workerPersPath, workerWithState.toByteArray());
      LOG.info("Worker status changed to: " + newStatus);
      return true;
    } catch (Exception e) {
      LOG.log(Level.SEVERE,
          "Could not update worker status in znode: " + workerInfo.getWorkerID(), e);
      return false;
    }
  }

  public static WorkerWithState getWorkerWithState(CuratorFramework client,
                                                   String workersPersDir,
                                                   int workerID) {

    String workerPersPath = ZKUtils.constructWorkerPath(workersPersDir, workerID);

    try {
      if (client.checkExists().forPath(workerPersPath) != null) {
        byte[] workerNodeBody = client.getData().forPath(workerPersPath);
        return WorkerWithState.decode(workerNodeBody);
      }

      return null;

    } catch (Exception e) {
      LOG.log(Level.SEVERE,
          "Could not get persistent worker znode data: " + workerPersPath, e);
      throw new Twister2RuntimeException(e);
    }
  }

  /**
   * return all registered workers
   */
  public static LinkedList<WorkerWithState> getWorkers(CuratorFramework client,
                                                       String rootPath,
                                                       String jobName) {

    String workersPersDir = ZKUtils.constructWorkersPersDir(rootPath, jobName);

    try {
      List<String> children = client.getChildren().forPath(workersPersDir);
      LinkedList<WorkerWithState> workers = new LinkedList();
      for (String childName : children) {
        String childPath = workersPersDir + "/" + childName;
        byte[] workerNodeBody = client.getData().forPath(childPath);
        WorkerWithState workerWithState = WorkerWithState.decode(workerNodeBody);
        workers.add(workerWithState);
      }

      return workers;
    } catch (Exception e) {
      LOG.log(Level.SEVERE,
          "Could not get persistent worker znode data: " + workersPersDir, e);
      throw new Twister2RuntimeException(e);
    }
  }


}
