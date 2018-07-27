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
import edu.iu.dsc.tws.common.discovery.IWorkerDiscoverer;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.bootstrap.JobMasterBasedWorkerDiscoverer;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.container.IWorker;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

public final class StandaloneWorkerStarter {
  private static final Logger LOG = Logger.getLogger(StandaloneWorkerStarter.class.getName());

  private StandaloneWorkerStarter() {
  }

  public static void main(String[] args) {
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
      Config config = loadConfigurations(cmd, rank);
      // normal worker
      LOG.log(Level.INFO, "A worker process is starting...");
      createWorker(config);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SubmitterMain", cmdOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    }
  }

  /**
   * Setup the command line options for the MPI process
   * @return cli options
   */
  private static Options setupOptions() {
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

  private static Config loadConfigurations(CommandLine cmd, int id) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String container = cmd.getOptionValue("container_class");
    String configDir = cmd.getOptionValue("config_dir");
    String clusterType = cmd.getOptionValue("cluster_type");
    String jobName = cmd.getOptionValue("job_name");

    LOG.log(Level.FINE, String.format("Initializing process with "
            + "twister_home: %s container_class: %s config_dir: %s cluster_type: %s",
        twister2Home, container, configDir, clusterType));

    Config config = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);

    Config workerConfig = Config.newBuilder().putAll(config).
        put(SchedulerContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SchedulerContext.CONTAINER_CLASS, container).
        put(SchedulerContext.TWISTER2_CONTAINER_ID, id).
        put(SchedulerContext.TWISTER2_CLUSTER_TYPE, clusterType).build();

    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobName, workerConfig);
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);
    job.getJobResources().getNoOfContainers();

    Config updatedConfig = JobUtils.overrideConfigs(job, config);
    updatedConfig = Config.newBuilder().putAll(updatedConfig).
        put(SchedulerContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SchedulerContext.CONTAINER_CLASS, container).
        put(SchedulerContext.TWISTER2_CONTAINER_ID, id).
        put(SchedulerContext.TWISTER2_CLUSTER_TYPE, clusterType).
        put(SchedulerContext.JOB_NAME, job.getJobName()).build();
    return updatedConfig;
  }

  private static void createWorker(Config config) {
    // lets create the resource plan
    IWorkerDiscoverer workerController = createWorkerController(config);
    WorkerNetworkInfo workerNetworkInfo = workerController.getWorkerNetworkInfo();

    String containerClass = SchedulerContext.containerClass(config);

    ResourcePlan resourcePlan = new ResourcePlan(SchedulerContext.clusterType(config),
        workerNetworkInfo.getWorkerID());

    try {
      Object object = ReflectionUtils.newInstance(containerClass);
      if (object instanceof IContainer) {
        IContainer container = (IContainer) object;
        // now initialize the container
        container.init(config, workerNetworkInfo.getWorkerID(), resourcePlan);
      } else if (object instanceof IWorker) {
        IWorker worker = (IWorker) object;
        worker.init(config, workerNetworkInfo.getWorkerID(), resourcePlan,
            workerController, null, null);
      }
      LOG.log(Level.FINE, "loaded container class: " + containerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.log(Level.SEVERE, String.format("failed to load the container class %s",
          containerClass), e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Create the resource plan
   * @param config config
   * @return
   */
  private static IWorkerDiscoverer createWorkerController(Config config) {
    // first get the worker id
    String indexEnv = System.getenv("NOMAD_ALLOC_INDEX");
    String idEnv = System.getenv("NOMAD_ALLOC_ID");

    int index = Integer.valueOf(indexEnv);

    initLogger(config, index);
    LOG.log(Level.INFO, String.format("Worker id = %s and index = %d", idEnv, index));

    Map<String, Integer> ports = getPorts(config);
    Map<String, String> localIps = getIPAddress(ports);

    String jobMasterIP = JobMasterContext.jobMasterIP(config);
    int masterPort = JobMasterContext.jobMasterPort(config);

    String jobName = StandaloneContext.jobName(config);
    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobName, config);
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);
    int numberContainers = job.getJobResources().getNoOfContainers();

    return new JobMasterBasedWorkerDiscoverer(config, index, numberContainers,
        jobMasterIP, masterPort, ports, localIps);
  }

  /**
   * Get the ports from the environment variable
   * @param cfg the configuration
   * @return port name -> port map
   */
  private static Map<String, Integer> getPorts(Config cfg) {
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
  private static void initLogger(Config cfg, int workerID) {
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

  private static String getTaskDirectory() {
    return System.getenv("NOMAD_TASK_DIR");
  }

  private static Map<String, String> getIPAddress(Map<String, Integer> ports) {
    Map<String, String> ips = new HashMap<>();
    for (Map.Entry<String, Integer> e : ports.entrySet()) {
      ips.put(e.getKey(), System.getenv("NOMAD_IP_" + e.getKey()));
    }
    return ips;
  }
}
