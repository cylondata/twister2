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
package edu.iu.dsc.tws.rsched.schedulers.standalone;

import java.io.File;
import java.nio.file.Paths;
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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.client.JobMasterClient;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public final class StandaloneWorkerStarter {
  private static final Logger LOG = Logger.getLogger(StandaloneWorkerStarter.class.getName());

  /**
   * The jobmaster client
   */
  private JobMasterClient masterClient;

  /**
   * Configuration
   */
  private Config config;

  /**
   * The worker controller
   */
  private IWorkerController workerController;

  private StandaloneWorkerStarter(String[] args) {
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
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SubmitterMain", cmdOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    }
  }

  public static void main(String[] args) {
    StandaloneWorkerStarter starter = new StandaloneWorkerStarter(args);
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

    Option jobName = Option.builder("j")
        .desc("Job name")
        .longOpt("job_name")
        .hasArgs()
        .argName("job name")
        .required()
        .build();
    options.addOption(twister2Home);
    options.addOption(containerClass);
    options.addOption(configDirectory);
    options.addOption(clusterType);
    options.addOption(jobName);

    return options;
  }

  private Config loadConfigurations(CommandLine cmd, int id) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String container = cmd.getOptionValue("container_class");
    String configDir = cmd.getOptionValue("config_dir");
    String clusterType = cmd.getOptionValue("cluster_type");
    String jobName = cmd.getOptionValue("job_name");

    LOG.log(Level.FINE, String.format("Initializing process with "
            + "twister_home: %s container_class: %s config_dir: %s cluster_type: %s",
        twister2Home, container, configDir, clusterType));

    Config cfg = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);

    Config workerConfig = Config.newBuilder().putAll(cfg).
        put(SchedulerContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SchedulerContext.WORKER_CLASS, container).
        put(SchedulerContext.TWISTER2_CONTAINER_ID, id).
        put(SchedulerContext.TWISTER2_CLUSTER_TYPE, clusterType).build();

    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobName, workerConfig);
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);
    job.getNumberOfWorkers();

    Config updatedConfig = JobUtils.overrideConfigs(job, cfg);
    updatedConfig = Config.newBuilder().putAll(updatedConfig).
        put(SchedulerContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SchedulerContext.WORKER_CLASS, container).
        put(SchedulerContext.TWISTER2_CONTAINER_ID, id).
        put(SchedulerContext.TWISTER2_CLUSTER_TYPE, clusterType).
        put(SchedulerContext.JOB_NAME, job.getJobName()).build();
    return updatedConfig;
  }

  private void startWorker() {
    LOG.log(Level.INFO, "A worker process is starting...");
    // lets create the resource plan
    this.workerController = createWorkerController();
    WorkerNetworkInfo workerNetworkInfo = workerController.getWorkerNetworkInfo();

    String workerClass = SchedulerContext.workerClass(config);

    AllocatedResources resourcePlan = new AllocatedResources(SchedulerContext.clusterType(config),
        workerNetworkInfo.getWorkerID());
    List<WorkerNetworkInfo> networkInfos = workerController.waitForAllWorkersToJoin(30000);
    for (WorkerNetworkInfo w : networkInfos) {
      WorkerComputeResource workerComputeResource = new WorkerComputeResource(w.getWorkerID());
      resourcePlan.addWorkerComputeResource(workerComputeResource);
    }

    try {
      Object object = ReflectionUtils.newInstance(workerClass);
      if (object instanceof IWorker) {
        IWorker container = (IWorker) object;
        // now initialize the container
        container.execute(config, workerNetworkInfo.getWorkerID(), resourcePlan,
            workerController, null, null);
      } else {
        throw new RuntimeException("Job is not of time IWorker: " + object.getClass().getName());
      }
      LOG.log(Level.FINE, "loaded worker class: " + workerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.log(Level.SEVERE, String.format("failed to load the worker class %s",
          workerClass), e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Create the resource plan
   * @return
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

    String jobMasterIP = JobMasterContext.jobMasterIP(config);
    int jobMasterPort = JobMasterContext.jobMasterPort(config);

    String jobName = StandaloneContext.jobName(config);
    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobName, config);
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);
    int numberOfWorkers = job.getNumberOfWorkers();

    int port = ports.get("worker");
    String host = localIps.get("worker");
    WorkerNetworkInfo networkInfo = new WorkerNetworkInfo(host, port, workerID);

    this.masterClient = createMasterClient(config, jobMasterIP, jobMasterPort,
        networkInfo, numberOfWorkers);

    return masterClient.getJMWorkerController();
  }

  /**
   * Create the job master client to get information about the workers
   */
  private JobMasterClient createMasterClient(Config cfg, String masterHost, int masterPort,
                                                    WorkerNetworkInfo networkInfo,
                                                    int numberContainers) {
    // we start the job master client
    JobMasterClient jobMasterClient = new JobMasterClient(cfg,
        networkInfo, masterHost, masterPort, numberContainers);
    LOG.log(Level.INFO, String.format("Connecting to job master %s:%d", masterHost, masterPort));
    jobMasterClient.startThreaded();
    // now lets send the starting message
    jobMasterClient.sendWorkerStartingMessage();

    // now lets send the starting message
    jobMasterClient.sendWorkerRunningMessage();

    return jobMasterClient;
  }

  /**
   * Get the ports from the environment variable
   * @param cfg the configuration
   * @return port name -> port map
   */
  private Map<String, Integer> getPorts(Config cfg) {
    String portNamesConfig = StandaloneContext.networkPortNames(cfg);
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
   * @param cfg the configuration
   * @param workerID worker id
   */
  private void initLogger(Config cfg, int workerID) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    // set logging level
    LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cfg));

    String persistentJobDir = getTaskDirectory();
    // if no persistent volume requested, return
    if (persistentJobDir == null) {
      return;
    }

    String jobWorkingDirectory = StandaloneContext.workingDirectory(cfg);
    String jobName = StandaloneContext.jobName(cfg);
    if (StandaloneContext.getLoggingSandbox(cfg)) {
      persistentJobDir = Paths.get(jobWorkingDirectory, jobName).toString();
    }

    String logDir = persistentJobDir + "/logs";
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
    masterClient.sendWorkerCompletedMessage();
    masterClient.close();
  }
}
