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

package edu.iu.dsc.tws.rsched.zk;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public final class ZKJobZnodeUtil {
  public static final Logger LOG = Logger.getLogger(ZKJobZnodeUtil.class.getName());

  private ZKJobZnodeUtil() { }

  /**
   * check whether there is an active job
   * if not, but there are znodes from previous sessions, those will be deleted
   */
  public static boolean isThereJobZNodes(CuratorFramework client, String rootPath, String jobName) {

    boolean jobZnodesExist = false;
    StringBuffer logMessage = new StringBuffer();

    try {
      // check whether the job node exists, if not, return false, nothing to do
      String jobPath = ZKUtils.constructJobPath(rootPath, jobName);
      if (client.checkExists().forPath(jobPath) != null) {
        jobZnodesExist = true;
        logMessage.append("jobZnode exists: " + jobPath);
      }

      // check whether the job node exists, if not, return false, nothing to do
      String daiPathForBarrier = ZKUtils.constructDaiPathForBarrier(rootPath, jobName);
      if (client.checkExists().forPath(daiPathForBarrier) != null) {
        jobZnodesExist = true;
        logMessage.append("\nJob daiPathForBarrier exists: " + daiPathForBarrier);
      }

      if (jobZnodesExist) {
        LOG.info(logMessage.toString());
      }

      return jobZnodesExist;

    } catch (Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return jobZnodesExist;
    }
  }

  /**
   * Create job znode with JobAPI.Job object as its payload
   * Assumes that there is no job znode exists in the ZooKeeper
   * This method should be called by the submitting client
   */
  public static void createJobZNode(CuratorFramework client, String rootPath, JobAPI.Job job)
      throws Exception {

    String jobPath = ZKUtils.constructJobPath(rootPath, job.getJobName());

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(jobPath, job.toByteArray());

      LOG.info("JobZNode created: " + jobPath);

    } catch (Exception e) {
      throw new Exception("JobZNode can not be created for the path: " + jobPath, e);
    }
  }

  /**
   * Create job znode with JobAPI.Job object as its payload
   * Assumes that there is no job znode exists in the ZooKeeper
   * This method should be called by the submitting client
   */
  public static JobAPI.Job readJobZNodeBody(CuratorFramework client, String jobName, Config config)
      throws Exception {

    String jobPath = ZKUtils.constructJobPath(ZKContext.rootNode(config), jobName);

    try {
      byte[] jobBytes = client.getData().forPath(jobPath);
      return decodeJobZnode(jobBytes);
    } catch (Exception e) {
      LOG.severe("Could not read job znode body: " + e.getMessage());
      throw e;
    }
  }

  /**
   * decode job znode body bytes
   */
  public static JobAPI.Job decodeJobZnode(byte[] body) throws InvalidProtocolBufferException {

    return JobAPI.Job.newBuilder()
        .mergeFrom(body, 0, body.length)
        .build();
  }

  /**
   * Create job znode with JobAPI.Job object as its payload
   * Assumes that there is no job znode exists in the ZooKeeper
   * This method should be called by the submitting client
   */
  public static void updateJobZNode(CuratorFramework client, JobAPI.Job job, String jobPath)
      throws Exception {

    try {
      client
          .setData()
          .forPath(jobPath, job.toByteArray());

      LOG.info("JobZNode Updated: " + jobPath);

    } catch (Exception e) {
      LOG.severe("Could not update job znode: " + e.getMessage());
      throw e;
    }
  }

  /**
   * delete all znodes related to the given jobName
   */
  public static boolean terminateJob(String zkServers, String rootPath, String jobName) {
    try {
      CuratorFramework client = ZKUtils.connectToServer(zkServers);
      boolean deleteResult = deleteJobZNodes(client, rootPath, jobName);
      client.close();
      return deleteResult;
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not delete job znodes", e);
      return false;
    }
  }

  /**
   * delete job related znode from previous sessions
   */
  public static boolean deleteJobZNodes(CuratorFramework client, String rootPath, String jobName) {
    try {
      String jobPath = ZKUtils.constructJobPath(rootPath, jobName);
      if (client.checkExists().forPath(jobPath) != null) {
        client.delete().deletingChildrenIfNeeded().forPath(jobPath);
        LOG.log(Level.INFO, "Job Znode deleted from ZooKeeper: " + jobPath);
      } else {
        LOG.log(Level.INFO, "No job znode exists in ZooKeeper to delete for: " + jobPath);
      }

      // delete distributed atomic integer for barrier
      String daiPath = ZKUtils.constructDaiPathForBarrier(rootPath, jobName);
      if (client.checkExists().forPath(daiPath) != null) {
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(daiPath);
        LOG.info("DistributedAtomicInteger for barrier deleted from ZooKeeper: " + daiPath);
      } else {
        LOG.info("No DistributedAtomicInteger exists for the job at ZooKeeper: " + daiPath);
      }

      return true;
    } catch (Exception e) {
      LOG.log(Level.WARNING, "", e);
      return false;
    }
  }



}
