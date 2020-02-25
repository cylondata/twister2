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
package edu.iu.dsc.tws.rsched.job;

import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.common.zk.JobWithState;
import edu.iu.dsc.tws.common.zk.JobZNodeManager;
import edu.iu.dsc.tws.common.zk.WorkerWithState;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKPersStateManager;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;

/**
 * list jobs from ZooKeeper server
 * get the details of a job from ZooKeeper server
 */
public final class ZKJobLister {
  private static final Logger LOG = Logger.getLogger(ZKJobLister.class.getName());

  private static Config config;

  private ZKJobLister() {
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      LOG.severe("Usage: java ZKJobLister jobs/jobID");
      return;
    }

    // load configurations
    config = ResourceAllocator.loadConfig(new HashMap<>());

    // if ZooKeeper server is not used, return. Nothing to be done.
    if (ZKContext.serverAddresses(config) == null) {
      LOG.severe("ZooKeeper server address is not provided in configuration files.");
      return;
    }

    if (args[0].equals("jobs")) {
      listJobs();
    } else {
      listJob(args[0]);
    }
  }

  /**
   * list jobs from ZooKeeper
   */
  public static void listJobs() {

    CuratorFramework client = ZKUtils.connectToServer(ZKContext.serverAddresses(config));
    String rootPath = ZKContext.rootNode(config);
    List<JobWithState> jobs;
    try {
      jobs = JobZNodeManager.getJobs(client, rootPath);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not get jobs from zookeeper", e);
      return;
    }

    if (jobs.size() == 0) {
      StringBuffer buffer = new StringBuffer();
      buffer.append("\nNumber of all jobs: " + jobs.size());
      LOG.info(buffer.toString());
      return;
    }

    int maxJobIdLength = jobs.stream()
        .mapToInt(j -> j.getJob().getJobId().length())
        .max()
        .orElseThrow(() -> new RuntimeException("No valid jobID in jobs"));

    List<JobWithState> finishedJobs = jobs.stream()
        .filter(j -> j.finished())
        .collect(Collectors.toList());

    List<JobWithState> activeJobs = jobs.stream()
        .filter(j -> j.active())
        .collect(Collectors.toList());

    int jobIDColumn = maxJobIdLength + 3;
    String format = "%-" + jobIDColumn + "s%-12s%s\n";

    int lineWidth = jobIDColumn + 12 + "Number of workers".length();
    String separator = StringUtils.repeat('=', lineWidth);

    StringBuilder buffer = new StringBuilder();
    Formatter f = new Formatter(buffer);
    f.format("\n\n%s", "Number of all jobs: " + jobs.size());
    f.format("\n%s", "");
    f.format("\n%s", "List of finished jobs: " + finishedJobs.size() + "\n");
    outputJobs(finishedJobs, f, format, separator);

    f.format("\n%s", "");
    f.format("\n%s", "List of active jobs: " + activeJobs.size() + "\n");
    outputJobs(activeJobs, f, format, separator);

    LOG.info(buffer.toString());
  }

  private static void outputJobs(List<JobWithState> jobs,
                                 Formatter f,
                                 String format,
                                 String separator) {
    f.format(format, "JobID", "JobState", "Number of workers");
    f.format("%s\n", separator);

    for (JobWithState jws : jobs) {
      f.format(format,
          jws.getJob().getJobId(),
          jws.getState().toString(),
          "" + jws.getJob().getNumberOfWorkers());
    }

  }

  /**
   * list job info from zk server
   * @param jobID
   */
  public static void listJob(String jobID) {
    CuratorFramework client = ZKUtils.connectToServer(ZKContext.serverAddresses(config));
    String rootPath = ZKContext.rootNode(config);

    JobWithState job;
    List<WorkerWithState> workers;
    try {
      job = JobZNodeManager.readJobZNode(client, rootPath, jobID);
      workers = ZKPersStateManager.getWorkers(client, rootPath, jobID);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Could not get the job from zookeeper: " + jobID, e);
      return;
    }

    int maxWorkerIPLength = workers.stream()
        .mapToInt(w -> w.getInfo().getWorkerIP().length())
        .max()
        .orElseThrow(() -> new RuntimeException("No valid workerIP in jobs"));

    StringBuilder buffer = new StringBuilder();
    Formatter f = new Formatter(buffer);
    f.format("\n\n%s", "JobID: " + job.getJob().getJobId());
    f.format("\n%s", "Job State: " + job.getState());
    f.format("\n%s", "Number of Workers: " + job.getJob().getNumberOfWorkers());
    f.format("\n%s", "");
    f.format("\n%s", "List of Workers: " + "\n");

    int workerIDColumn = "WorkerID".length() + 3;
    int workerIPColumn = maxWorkerIPLength + 3;
    String format = "%-" + workerIDColumn + "s%-" + workerIPColumn + "s%s\n";

    int lineWidth = workerIDColumn + workerIPColumn + "Worker State".length();
    String separator = StringUtils.repeat('=', lineWidth);

    f.format(format, "WorkerID", "WorkerIP", "Worker State");
    f.format("%s\n", separator);

    for (WorkerWithState wws : workers) {
      f.format(format,
          "" + wws.getWorkerID(),
          wws.getInfo().getWorkerIP(),
          wws.getState().toString());
    }

    LOG.info(buffer.toString());
  }

}
