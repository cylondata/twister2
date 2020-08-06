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
import java.util.logging.Logger;

import com.google.common.primitives.Longs;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.JobUtils;

public final class JobZNodeManager {
  public static final Logger LOG = Logger.getLogger(JobZNodeManager.class.getName());

  private JobZNodeManager() {
  }

  /**
   * Create job znode
   * Assumes that there is no znode exists in the ZooKeeper
   * Add job object as its body
   */
  public static void createJobZNode(CuratorFramework client, String rootPath, JobAPI.Job job) {

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
      throw new Twister2RuntimeException("Job ZNode can not be created for the path: " + jobDir, e);
    }
  }

  /**
   * read the body of job directory znode
   * decode and return
   */
  public static JobWithState readJobZNode(CuratorFramework client, String rootPath, String jobID) {

    String jobDir = ZKUtils.jobDir(rootPath, jobID);

    try {
      byte[] jobBytes = client.getData().forPath(jobDir);
      return JobWithState.decode(jobBytes);
    } catch (Exception e) {
      throw new Twister2RuntimeException("Could not read job znode body: " + e.getMessage(), e);
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
        LOG.fine("Job Znode exists: " + jobDir);
        return true;
      }

      return false;

    } catch (Exception e) {
      throw new Twister2RuntimeException("Could not check job znode existence.", e);
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
   * delete job znode from zk server
   */
  public static void deleteJobZNode(CuratorFramework client, String rootPath, String jobID) {

    try {
      String jobDir = ZKUtils.jobDir(rootPath, jobID);
      if (client.checkExists().forPath(jobDir) != null) {
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(jobDir);
        LOG.info("JobDirectory deleted from ZooKeeper: " + jobDir);
      } else {
        LOG.info("JobDirectory does not exist at ZooKeeper: " + jobDir);
      }
    } catch (Exception e) {
      throw new Twister2RuntimeException("Can not delete job znode.");
    }
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

  /**
   * Create job submission time znode
   */
  public static void createJstZNode(CuratorFramework client,
                                    String rootPath,
                                    String jobID,
                                    long jsTime) {

    String jstPath = ZKUtils.jobSubmisionTimePath(rootPath, jobID);

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(jstPath, Longs.toByteArray(jsTime));

      LOG.info("Job Submission Time ZNode created: " + jstPath);

    } catch (Exception e) {
      throw new Twister2RuntimeException("Can not create job submission time znode: " + jstPath, e);
    }
  }

  /**
   * Create job end time znode
   */
  public static void createJobEndTimeZNode(CuratorFramework client,
                                           String rootPath,
                                           String jobID) {

    String endTimePath = ZKUtils.jobEndTimePath(rootPath, jobID);
    long endTime = System.currentTimeMillis();

    try {
      client
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT)
          .forPath(endTimePath, Longs.toByteArray(endTime));

      LOG.info("Job End Time ZNode created: " + endTimePath);

    } catch (Exception e) {
      throw new Twister2RuntimeException("Can not create job end time znode: " + endTimePath, e);
    }
  }

  /**
   * Job master creates job submission time znode under job znode,
   * as the last action to create job related znodes at ZK server
   * workers wait for the job master to create this znode.
   * They proceed after seeing that this jst znode is created
   * <p>
   * this jst znode may exist from previous runs in the case of restarting from a checkpoint
   * because of this, its value has to be compared
   */
  public static boolean checkJstZNodeWaitIfNeeded(CuratorFramework client,
                                                  String rootPath,
                                                  String jobID,
                                                  long jsTime) throws Twister2Exception {

    String jstPath = ZKUtils.jobSubmisionTimePath(rootPath, jobID);

    long timeLimit = 100000; // 100 seconds
    long sleepInterval = 300;
    long duration = 0;
    long startTime = System.currentTimeMillis();

    // log interval in milliseconds
    long logInterval = 3000;
    long nextLogTime = logInterval;
    int checkCount = 1;

    while (duration < timeLimit) {
      try {
        if (client.checkExists().forPath(jstPath) != null) {
          byte[] jstBytes = client.getData().forPath(jstPath);
          long jstAtZK = Longs.fromByteArray(jstBytes);
          if (jstAtZK == jsTime) {
            LOG.info("matched job submission times. Proceeding. checkCount: " + checkCount);
            return true;
          }
        }
      } catch (Exception e) {
        throw new Twister2Exception("Can not get job submission znode data.", e);
      }

      try {
        Thread.sleep(sleepInterval);
      } catch (InterruptedException e) {
        LOG.warning("Sleeping thread interrupted.");
      }

      duration = System.currentTimeMillis() - startTime;
      checkCount++;

      if (duration > nextLogTime) {
        LOG.info("Still waiting for job submission time znode to be created: " + jstPath);
        nextLogTime += logInterval;
      }
    }

    throw new Twister2Exception("Job Submission Time znode is not created by job master "
        + "on the time limit: " + timeLimit + " ms");
  }

}
