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
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.JobUtils;

public final class JobZNodeManager {
  public static final Logger LOG = Logger.getLogger(JobZNodeManager.class.getName());

  private static final long MAX_WAIT_TIME_FOR_ZNODE_DELETE = 5000;

  private JobZNodeManager() {
  }

  /**
   * Create job znode
   * Assumes that there is no znode exists in the ZooKeeper
   * Add job object as its body
   */
  public static void createJobZNode(CuratorFramework client, String rootPath, JobAPI.Job job)
      throws Twister2Exception {

    String jobDir = ZKUtils.jobDir(rootPath, job.getJobId());
    JobWithState jobWithState = new JobWithState(job, JobAPI.JobState.STARTING);

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(jobDir, jobWithState.toByteArray());

      LOG.info("Job ZNode created: " + jobDir);

    } catch (Exception e) {
      throw new Twister2Exception("Job ZNode can not be created for the path: " + jobDir, e);
    }
  }

  /**
   * read the body of job directory znode
   * decode and return
   */
  public static JobWithState readJobZNode(CuratorFramework client, String rootPath, String jobID)
      throws Twister2Exception {

    String jobDir = ZKUtils.jobDir(rootPath, jobID);

    try {
      byte[] jobBytes = client.getData().forPath(jobDir);
      return JobWithState.decode(jobBytes);
    } catch (Exception e) {
      throw new Twister2Exception("Could not read job znode body: " + e.getMessage(), e);
    }
  }

  /**
   * check whether there is an active job
   */
  public static boolean isThereJobZNode(CuratorFramework clnt, String rootPath, String jobID) {

    try {
      // check whether the job znode exists, if not, return false, nothing to do
      String jobDir = ZKUtils.jobDir(rootPath, jobID);
      if (clnt.checkExists().forPath(jobDir) != null) {
        ZKUtils.LOG.info("Job Znode exists: " + jobDir);
        return true;
      }

      return false;

    } catch (Exception e) {
      ZKEphemStateManager.LOG.log(Level.SEVERE, e.getMessage(), e);
      return false;
    }
  }

  public static boolean updateJob(CuratorFramework client,
                                  String rootPath,
                                  JobAPI.Job job,
                                  JobAPI.JobState state) throws Twister2Exception {

    String jobDir = ZKUtils.jobDir(rootPath, job.getJobId());
    JobWithState jobWithState = new JobWithState(job, state);

    try {
      client.setData().forPath(jobDir, jobWithState.toByteArray());
      return true;
    } catch (Exception e) {
      throw new Twister2Exception("Could not update Job in znode: " + jobDir, e);
    }
  }

  public static boolean updateJobState(CuratorFramework client,
                                       String rootPath,
                                       String jobID,
                                       JobAPI.JobState state) throws Twister2Exception {
    JobWithState jobWithState = readJobZNode(client, rootPath, jobID);
    boolean updated = updateJob(client, rootPath, jobWithState.getJob(), state);
    LOG.info("Job state changed to: " + state);
    return updated;
  }

  /**
   * delete job related znode from previous sessions
   */
  public static boolean deleteJobZNodes(CuratorFramework clnt, String rootPath, String jobID) {

    boolean allDeleted = true;

    // delete workers ephemeral znode directory
    String jobPath = ZKUtils.ephemDir(rootPath, jobID);
    try {
      if (clnt.checkExists().forPath(jobPath) != null) {

        // wait for workers to be deleted
        long delay = 0;
        long start = System.currentTimeMillis();
        List<String> list = clnt.getChildren().forPath(jobPath);
        int children = list.size();

        while (children != 0 && delay < MAX_WAIT_TIME_FOR_ZNODE_DELETE) {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
          }

          delay = System.currentTimeMillis() - start;
          list = clnt.getChildren().forPath(jobPath);
          children = list.size();
        }

        if (list.size() != 0) {
          ZKUtils.LOG.info("Waited " + delay + " ms before deleting job znode. Children: " + list);
        }

        clnt.delete().deletingChildrenIfNeeded().forPath(jobPath);
        ZKUtils.LOG.log(Level.INFO, "Job Znode deleted from ZooKeeper: " + jobPath);
      } else {
        ZKUtils.LOG.log(Level.INFO, "No job znode exists in ZooKeeper to delete for: " + jobPath);
      }
    } catch (Exception e) {
      ZKUtils.LOG.log(Level.FINE, "", e);
      ZKUtils.LOG.info("Following exception is thrown when deleting the job znode: " + jobPath
          + "; " + e.getMessage());
      allDeleted = false;
    }

    try {
      // delete job directory
      String jobDir = ZKUtils.jobDir(rootPath, jobID);
      if (clnt.checkExists().forPath(jobDir) != null) {
        clnt.delete().guaranteed().deletingChildrenIfNeeded().forPath(jobDir);
        ZKUtils.LOG.info("JobDirectory deleted from ZooKeeper: " + jobDir);
      } else {
        ZKUtils.LOG.info("JobDirectory does not exist at ZooKeeper: " + jobDir);
      }
    } catch (Exception e) {
      ZKUtils.LOG.log(Level.WARNING, "", e);
      allDeleted = false;
    }

    return allDeleted;
  }

  /**
   * Update number of workers in the job object
   * this is called in case of scaling up/down
   */
  public static void updateJobWorkers(CuratorFramework client,
                                      String rootPath,
                                      String jobID,
                                      int workerChange)
      throws Twister2Exception {

    JobWithState jobWithState = readJobZNode(client, rootPath, jobID);
    // update the job object
    JobAPI.Job updatedJob = JobUtils.scaleJob(jobWithState.getJob(), workerChange);
    updateJob(client, rootPath, updatedJob, jobWithState.getState());
    LOG.info("NumberOfWorkers in Job updated to: " + updatedJob.getNumberOfWorkers());
  }

  /**
   * return all jobs
   */
  public static List<JobWithState> getJobs(CuratorFramework client,
                                           String rootPath) throws Twister2Exception {

    try {
      List<String> jobPaths = client.getChildren().forPath(rootPath);
      LinkedList<JobWithState> jobs = new LinkedList();
      for (String jobID : jobPaths) {
        String childPath = rootPath + "/" + jobID;
        byte[] jobZNodeBody = client.getData().forPath(childPath);
        JobWithState jobWithState = JobWithState.decode(jobZNodeBody);
        jobs.add(jobWithState);
      }

      return jobs;
    } catch (Exception e) {
      throw new Twister2Exception("Could not get job znode data: " + rootPath, e);
    }
  }


}
