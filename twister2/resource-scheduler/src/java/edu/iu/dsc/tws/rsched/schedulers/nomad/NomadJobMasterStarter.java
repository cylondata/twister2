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
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.driver.IScalerPerCluster;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.scheduler.SchedulerContext;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.zk.ZKJobMasterRegistrar;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.utils.ResourceSchedulerUtils;

//import edu.iu.dsc.tws.rsched.bootstrap.ZKJobMasterRegistrar;


public final class NomadJobMasterStarter {
  private static final Logger LOG = Logger.getLogger(NomadJobMasterStarter.class.getName());

  private JobAPI.Job job;
  private Config config;
  private NomadController controller;

  public NomadJobMasterStarter(String[] args) {
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
        .desc("Job Id")
        .longOpt("job_id")
        .hasArgs()
        .argName("job id")
        .required()
        .build();
    Option jobId = Option.builder("i")
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
    options.addOption(jobId);

    return options;
  }

  private Config loadConfigurations(CommandLine cmd, int id) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String container = cmd.getOptionValue("container_class");
    String configDir = cmd.getOptionValue("config_dir");
    String clusterType = cmd.getOptionValue("cluster_type");
    String jobId = cmd.getOptionValue("job_id");

    LOG.log(Level.FINE, String.format("Initializing process with "
            + "twister_home: %s container_class: %s config_dir: %s cluster_type: %s",
        twister2Home, container, configDir, clusterType));

    Config cfg = ConfigLoader.loadConfig(twister2Home, configDir, clusterType);

    Config workerConfig = Config.newBuilder().putAll(cfg).
        put(SchedulerContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SchedulerContext.WORKER_CLASS, container).
        put(SchedulerContext.TWISTER2_CONTAINER_ID, id).
        put(SchedulerContext.JOB_ID, jobId).
        put(SchedulerContext.TWISTER2_CLUSTER_TYPE, clusterType).build();

    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobId, workerConfig);
    job = JobUtils.readJobFile(null, jobDescFile);
    job.getNumberOfWorkers();

    Config updatedConfig = JobUtils.overrideConfigs(job, cfg);
    updatedConfig = Config.newBuilder().putAll(updatedConfig).
        put(SchedulerContext.TWISTER2_HOME.getKey(), twister2Home).
        put(SchedulerContext.WORKER_CLASS, container).
        put(SchedulerContext.TWISTER2_CONTAINER_ID, id).
        put(SchedulerContext.TWISTER2_CLUSTER_TYPE, clusterType).
        put(SchedulerContext.JOB_ID, jobId).
        put(SchedulerContext.JOB_NAME, job.getJobName()).build();
    return updatedConfig;
  }

  public void initialize(JobAPI.Job jb, Config cfg) {
    job = jb;
    config = cfg;
  }

  public static void main(String[] args) {
    NomadJobMasterStarter starter = new NomadJobMasterStarter(args);
    starter.run();
  }

  public void run() {
    // normal worker
    try {
      launch();
    } finally {
      // now close the worker
      //closeWorker();
    }
  }

  /**
   * launch the job master
   *
   * @return false if setup fails
   */
  public boolean launch() {
    // get the job working directory
/*    String jobWorkingDirectory = NomadContext.workingDirectory(config);
    LOG.log(Level.INFO, "job working directory ....." + jobWorkingDirectory);

    if (NomadContext.sharedFileSystem(config)) {
      if (!setupWorkingDirectory(job, jobWorkingDirectory)) {
        throw new RuntimeException("Failed to setup the directory");
      }
    }

    Config newConfig = Config.newBuilder().putAll(config).put(
        SchedulerContext.WORKING_DIRECTORY, jobWorkingDirectory).build();
    // now start the controller, which will get the resources from
    // slurm and start the job
    //IController controller = new NomadController(true);
    controller.initialize(newConfig);*/
    String indexEnv = System.getenv("NOMAD_ALLOC_INDEX");
    String idEnv = System.getenv("NOMAD_ALLOC_ID");

    int workerID = Integer.valueOf(indexEnv);

    initLogger(config, workerID);
    LOG.log(Level.INFO, String.format("Worker id = %s and index = %d", idEnv, workerID));
    ZKJobMasterRegistrar registrar = null;
    int port = JobMasterContext.jobMasterPort(config);
    String hostAddress = null;
    try {
      hostAddress = Inet4Address.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    try {
      registrar = new ZKJobMasterRegistrar(config,
          hostAddress, port, job.getJobId());
      LOG.info("JobMaster REGISTERED..:" + hostAddress);
    } catch (Exception e) {
      LOG.info("JobMaster CAN NOT BE REGISTERED:");
      e.printStackTrace();
    }
    boolean initialized = registrar.initialize();
    if (!initialized) {
      LOG.info("CAN NOT INITIALIZE");
    }
    if (!initialized && registrar.sameZNodeExist()) {
      registrar.deleteJobMasterZNode();
      registrar.initialize();
    }
    // start the Job Master locally
    JobMaster jobMaster = null;
    JobMasterAPI.NodeInfo jobMasterNodeInfo = NomadContext.getNodeInfo(config, hostAddress);
    IScalerPerCluster clusterScaler = null;
    Thread jmThread = null;
    int workerCount = job.getNumberOfWorkers();
    LOG.info("Worker Count..: " + workerCount);

    //if you want to set it manually
    //if (JobMasterContext.jobMasterIP(config) != null) {
    //  hostAddress = JobMasterContext.jobMasterIP(config);
    //}
    LOG.log(Level.INFO, String.format("Starting the Job Master: %s:%d", hostAddress, port));
    JobMasterAPI.JobMasterState initialState = JobMasterAPI.JobMasterState.JM_STARTED;
    NomadTerminator nt = new NomadTerminator();

    jobMaster = new JobMaster(
        config, hostAddress, nt, job, jobMasterNodeInfo, clusterScaler, initialState);
    jobMaster.addShutdownHook(true);
    try {
      jobMaster.startJobMasterBlocking();
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
    //jmThread = jobMaster.startJobMasterThreaded();

    waitIndefinitely();
    registrar.deleteJobMasterZNode();
    registrar.close();

    boolean start = controller.start(job);
    // now lets wait on client
//    if (JobMasterContext.jobMasterRunsInClient(config)) {
//      try {
//        if (jmThread != null) {
//          jmThread.join();
//        }
//      } catch (InterruptedException ignore) {
//      }
//    }
    return start;
  }

  /**
   * a method to make the job master wait indefinitely
   */
  public static void waitIndefinitely() {

    while (true) {
      try {
        LOG.info("JobMasterStarter thread waiting indefinitely. Sleeping 100sec. "
            + "Time: " + new java.util.Date());
        Thread.sleep(100000);
      } catch (InterruptedException e) {
        LOG.warning("Thread sleep interrupted.");
      }
    }
  }

  /**
   * setup the working directory mainly it downloads and extracts the heron-core-release
   * and job package to the working directory
   *
   * @return false if setup fails
   */
  private boolean setupWorkingDirectory(JobAPI.Job jb, String jobWorkingDirectory) {
    // get the path of core release URI
    String corePackage = NomadContext.corePackageFileName(config);
    String jobPackage = NomadContext.jobPackageFileName(config);
    LOG.log(Level.INFO, "Core Package is ......: " + corePackage);
    LOG.log(Level.INFO, "Job Package is ......: " + jobPackage);
    // Form the job package's URI
    String jobPackageURI = NomadContext.jobPackageUri(config).toString();
    LOG.log(Level.INFO, "Job Package URI is ......: " + jobPackageURI);
    // copy the files to the working directory
    return ResourceSchedulerUtils.setupWorkingDirectory(
        jb.getJobId(),
        jobWorkingDirectory,
        corePackage,
        jobPackageURI,
        Context.verbose(config));
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
    //String jobID = NomadContext.jobId(cfg);
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

}
