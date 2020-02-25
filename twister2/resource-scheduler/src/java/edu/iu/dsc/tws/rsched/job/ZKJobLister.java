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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.zk.JobWithState;
import edu.iu.dsc.tws.common.zk.JobZNodeManager;
import edu.iu.dsc.tws.common.zk.WorkerWithState;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKPersStateManager;
import edu.iu.dsc.tws.common.zk.ZKUtils;

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

    Options cmdOptions = null;
    try {
      cmdOptions = setupOptions();
      CommandLineParser parser = new DefaultParser();
      // parse the help options first.
      CommandLine cmd = parser.parse(cmdOptions, args);

      // load the configuration
      // we are loading the configuration for all the components
      config = loadConfigurations(cmd);
      // normal worker
      String command = cmd.getOptionValue("command");
      String jobID = Context.jobId(config);
      LOG.log(Level.FINE, "command: " + command);
      LOG.log(Level.FINE, "jobID: " + jobID);

      // if ZooKeeper server is not used, return. Nothing to be done.
      if (ZKContext.serverAddresses(config) == null) {
        LOG.severe("ZooKeeper server address is not provided in configuration files.");
        return;
      }

      if ("jobs".equals(jobID)) {
        listJobs();
      } else {
        listJob(jobID);
      }

    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SubmitterMain", cmdOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    } catch (Throwable t) {
      String msg = "Un-expected error";
      LOG.log(Level.SEVERE, msg, t);
      throw new RuntimeException(msg, t);
    }
  }

  public static Config loadConfigurations(CommandLine cmd) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String configDir = cmd.getOptionValue("config_path");
    String jobID = cmd.getOptionValue("job_id");

    Config conf = ConfigLoader.loadConfig(twister2Home, configDir);

    return Config.newBuilder()
        .putAll(conf)
        .put(Context.TWISTER2_HOME.getKey(), twister2Home)
        .put(SchedulerContext.CONFIG_DIR, configDir)
        .put(Context.JOB_ID, jobID)
        .build();
  }

  /**
   * Setup the command line options for the MPI process
   *
   * @return cli options
   */
  public static Options setupOptions() {
    Options options = new Options();

    Option configDirectory = Option.builder("d")
        .desc("The config directory")
        .longOpt("config_path")
        .hasArgs()
        .argName("configuration directory")
        .required()
        .build();

    Option twister2Home = Option.builder("t")
        .desc("The class name of the container to launch")
        .longOpt("twister2_home")
        .hasArgs()
        .argName("twister2 home")
        .required()
        .build();

    Option command = Option.builder("m")
        .desc("Command name")
        .longOpt("command")
        .hasArgs()
        .argName("command")
        .required()
        .build();

    Option jobID = Option.builder("j")
        .desc("Job id")
        .longOpt("job_id")
        .hasArgs()
        .argName("job id")
        .required()
        .build();
    options.addOption(twister2Home);
    options.addOption(configDirectory);
    options.addOption(command);
    options.addOption(jobID);

    return options;
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
