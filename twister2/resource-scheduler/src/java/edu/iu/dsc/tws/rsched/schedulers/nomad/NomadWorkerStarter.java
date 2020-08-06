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
package edu.iu.dsc.tws.rsched.schedulers.nomad;

import java.io.File;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.zk.ZKJobMasterFinder;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.worker.MPIWorkerManager;

//import edu.iu.dsc.tws.rsched.bootstrap.ZKJobMasterFinder;

public final class NomadWorkerStarter {
  private static final Logger LOG = Logger.getLogger(NomadWorkerStarter.class.getName());
  private static int startingPort = 30000;
  private NomadController controller;
  /**
   * The jobmaster client
   */
  private JMWorkerAgent masterClient;

  /**
   * Configuration
   */
  private Config config;

  /**
   * The worker controller
   */
  private IWorkerController workerController;

  private JobAPI.Job job;

  private NomadWorkerStarter(String[] args) {
    Options cmdOptions = null;
    try {
      cmdOptions = setupOptions();
      CommandLineParser parser = new DefaultParser();
      // parse the help options first.
      CommandLine cmd = parser.parse(cmdOptions, args);

      // lets determine the process id
      int rank = 0;

      // load the configuration
      // we are loading the configuration for all the components
      this.config = loadConfigurations(cmd, rank);
      controller = new NomadController(true);
      controller.initialize(config);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SubmitterMain", cmdOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    }
  }

  public static void main(String[] args) {
    NomadWorkerStarter starter = new NomadWorkerStarter(args);
    starter.run();
  }

  public void run() {
    // normal worker
    try {
      startWorker();
    } finally {
      // now close the worker
      closeWorker();
    }
  }

  /**
   * Setup the command line options for the MPI process
   *
   * @return cli options
   */
  private Options setupOptions() {
    Options options = new Options();

    Option containerClass = Option.builder("c")
        .desc("The class name of the container to launch")
        .longOpt("container_class")
        .hasArgs()
        .argName("container class")
        .required()
        .build();

    Option configDirectory = Option.builder("d")
        .desc("The class name of the container to launch")
        .longOpt("config_dir")
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

    Option clusterType = Option.builder("n")
        .desc("The clustr type")
        .longOpt("cluster_type")
        .hasArgs()
        .argName("cluster type")
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
    options.addOption(containerClass);
    options.addOption(configDirectory);
    options.addOption(clusterType);
    options.addOption(jobID);

    return options;
  }

  private Config loadConfigurations(CommandLine cmd, int id) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String container = cmd.getOptionValue("container_class");
    String configDir = cmd.getOptionValue("config_dir");
    String clusterType = cmd.getOptionValue("cluster_type");
    String jobID = cmd.getOptionValue("job_id");

    LOG.log(Level.FINE, String.format("Initializing process with "
            + "twister_home: %s container_class: %s config_dir: %s cluster_type: %s",
        twister2Home, container, configDir, clusterType));

    Config cfg = ConfigLoader.loadConfig(twister2Home, configDir, clusterType);

    Config workerConfig = Config.newBuilder().putAll(cfg).
        put(SchedulerContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SchedulerContext.WORKER_CLASS, container).
        put(SchedulerContext.TWISTER2_CONTAINER_ID, id).
        put(SchedulerContext.TWISTER2_CLUSTER_TYPE, clusterType).build();

    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobID, workerConfig);
    job = JobUtils.readJobFile(jobDescFile);
    job.getNumberOfWorkers();

    Config updatedConfig = JobUtils.overrideConfigs(job, cfg);
    updatedConfig = Config.newBuilder().putAll(updatedConfig).
        put(SchedulerContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SchedulerContext.WORKER_CLASS, container).
        put(SchedulerContext.TWISTER2_CONTAINER_ID, id).
        put(SchedulerContext.TWISTER2_CLUSTER_TYPE, clusterType).
        put(SchedulerContext.JOB_ID, job.getJobId()).
        put(SchedulerContext.JOB_NAME, job.getJobName()).build();
    return updatedConfig;
  }

  private void startWorker() {
    LOG.log(Level.INFO, "A worker process is starting...");
    // lets create the resource plan
    this.workerController = createWorkerController();
    JobMasterAPI.WorkerInfo workerNetworkInfo = workerController.getWorkerInfo();

    try {
      LOG.log(Level.INFO, "Worker IP..:" + Inet4Address.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    try {
      List<JobMasterAPI.WorkerInfo> workerInfos = workerController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }

    IWorker worker = JobUtils.initializeIWorker(job);
    MPIWorkerManager workerManager = new MPIWorkerManager();
    workerManager.execute(config, job, workerController, null, null, worker);
  }

  /**
   * Create the resource plan
   */
  private IWorkerController createWorkerController() {
    // first get the worker id
    String indexEnv = System.getenv("NOMAD_ALLOC_INDEX");
    String idEnv = System.getenv("NOMAD_ALLOC_ID");

    int workerID = Integer.valueOf(indexEnv);

    initLogger(config, workerID);
    LOG.log(Level.INFO, String.format("Worker id = %s and index = %d", idEnv, workerID));

    Map<String, Integer> ports = getPorts(config);
    Map<String, String> localIps = getIPAddress(ports);

    int numberOfWorkers = job.getNumberOfWorkers();
    LOG.info("Worker Count..: " + numberOfWorkers);
    JobAPI.ComputeResource computeResource = JobUtils.getComputeResource(job, 0);
    //Map<String, Integer> additionalPorts =
    //    NomadContext.generateAdditionalPorts(config, startingPort);
    int port = ports.get("worker");
    String host = localIps.get("worker");
    JobMasterAPI.NodeInfo nodeInfo = NomadContext.getNodeInfo(config, host);
    JobMasterAPI.WorkerInfo workerInfo =
        WorkerInfoUtils.createWorkerInfo(workerID, host, port, nodeInfo,
            computeResource, ports);

    int jobMasterPort = 0;
    String jobMasterIP = null;

    //find the jobmaster
    if (!JobMasterContext.jobMasterRunsInClient(config)) {
      ZKJobMasterFinder finder = new ZKJobMasterFinder(config, job.getJobId());
      finder.initialize();

      String jobMasterIPandPort = finder.getJobMasterIPandPort();
      if (jobMasterIPandPort == null) {
        LOG.info("Job Master has not joined yet. Will wait and try to get the address ...");
        jobMasterIPandPort = finder.waitAndGetJobMasterIPandPort(20000);
        LOG.info("Job Master address: " + jobMasterIPandPort);
      } else {
        LOG.info("Job Master address: " + jobMasterIPandPort);
      }

      finder.close();

      String jobMasterPortStr = jobMasterIPandPort.substring(jobMasterIPandPort.lastIndexOf(":")
          + 1);
      jobMasterPort = Integer.parseInt(jobMasterPortStr);
      jobMasterIP = jobMasterIPandPort.substring(0, jobMasterIPandPort.lastIndexOf(":"));
    } else {
      jobMasterIP = JobMasterContext.jobMasterIP(config);
      jobMasterPort = JobMasterContext.jobMasterPort(config);
    }

    config = JobUtils.overrideConfigs(job, config);
    config = JobUtils.updateConfigs(job, config);

    int workerCount = job.getNumberOfWorkers();
    LOG.info("Worker Count..: " + workerCount);

    this.masterClient = createMasterAgent(config, jobMasterIP, jobMasterPort,
        workerInfo, numberOfWorkers);

    return masterClient.getJMWorkerController();
  }

  /**
   * Create the job master client to get information about the workers
   */
  private JMWorkerAgent createMasterAgent(Config cfg, String masterHost, int masterPort,
                                          JobMasterAPI.WorkerInfo workerInfo,
                                          int numberContainers) {

    //TODO: zero means starting for the first time
    int restartCount = 0;

    // we start the job master client
    JMWorkerAgent jobMasterAgent = JMWorkerAgent.createJMWorkerAgent(cfg,
        workerInfo, masterHost, masterPort, numberContainers, restartCount);
    LOG.log(Level.INFO, String.format("Connecting to job master..: %s:%d", masterHost, masterPort));

    jobMasterAgent.startThreaded();
    // No need for sending workerStarting message anymore
    // that is called in startThreaded method

    return jobMasterAgent;
  }

  /**
   * Get the ports from the environment variable
   *
   * @param cfg the configuration
   * @return port name -> port map
   */
  private Map<String, Integer> getPorts(Config cfg) {
    String portNamesConfig = NomadContext.networkPortNames(cfg);
    String[] portNames = portNamesConfig.split(",");
    Map<String, Integer> ports = new HashMap<>();
    // now lets get these ports
    for (String pName : portNames) {
      String portNumber = System.getenv("NOMAD_PORT_" + pName);
      int port = Integer.valueOf(portNumber);
      ports.put(pName, port);
    }
    return ports;
  }

  /**
   * Initialize the loggers to log into the task local directory
   *
   * @param cfg the configuration
   * @param workerID worker id
   */
  private void initLogger(Config cfg, int workerID) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    String jobWorkingDirectory = NomadContext.workingDirectory(cfg);
    String jobID = NomadContext.jobId(cfg);

    NomadPersistentVolume pv =
        new NomadPersistentVolume(controller.createPersistentJobDirName(jobID), workerID);
    String persistentJobDir = pv.getJobDir().getAbsolutePath();
    //LOG.log(Level.INFO, "PERSISTENT LOG DIR is ......: " + persistentJobDir);
    //String persistentJobDir = getTaskDirectory();
    // if no persistent volume requested, return
    if (persistentJobDir == null) {
      return;
    }

//    if (NomadContext.getLoggingSandbox(cfg)) {
//      persistentJobDir = Paths.get(jobWorkingDirectory, jobID).toString();
//    }
    //nfs/shared/twister2/
    //String logDir = "/etc/nomad.d/"; //"/nfs/shared/twister2" + "/logs";
    String logDir = persistentJobDir + "/logs";

    LOG.log(Level.INFO, "LOG DIR is ......: " + logDir);
    File directory = new File(logDir);
    if (!directory.exists()) {
      if (!directory.mkdirs()) {
        throw new RuntimeException("Failed to create log directory: " + logDir);
      }
    }
    LoggingHelper.setupLogging(cfg, logDir, "worker-" + workerID);
  }

  private String getTaskDirectory() {
    return System.getenv("NOMAD_TASK_DIR");
  }

  private Map<String, String> getIPAddress(Map<String, Integer> ports) {
    Map<String, String> ips = new HashMap<>();
    for (Map.Entry<String, Integer> e : ports.entrySet()) {
      ips.put(e.getKey(), System.getenv("NOMAD_IP_" + e.getKey()));
    }
    return ips;
  }

  /**
   * last method to call to close the worker
   */
  public void closeWorker() {

    // send worker completed message to the Job Master and finish
    // Job master will delete the StatefulSet object
    masterClient.sendWorkerCompletedMessage(JobMasterAPI.WorkerState.COMPLETED);
    masterClient.close();
  }

}
