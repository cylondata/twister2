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
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.driver.IScalerPerCluster;
import edu.iu.dsc.tws.api.exceptions.Twister2Exception;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.api.resource.FSPersistentVolume;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.util.JSONUtils;
import edu.iu.dsc.tws.common.util.NetworkUtils;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.master.worker.JMSenderToDriver;
import edu.iu.dsc.tws.master.worker.JMWorkerAgent;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.JobExecutionState;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;
import edu.iu.dsc.tws.rsched.core.WorkerRuntime;
import edu.iu.dsc.tws.rsched.schedulers.nomad.NomadContext;
import edu.iu.dsc.tws.rsched.schedulers.nomad.NomadTerminator;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

/**
 * This is the base process started by the resource scheduler. This process will lanch the container
 * code and it will eventually will load the tasks.
 */
public final class MPIWorker {
  private static final Logger LOG = Logger.getLogger(MPIWorker.class.getName());

  /**
   * The jobmaster client
   */
  private JMWorkerAgent masterClient;

  /**
   * Configuration
   */
  private Config config;

  /**
   * Information of this worker
   */
  private JobMasterAPI.WorkerInfo wInfo;

  public void finalizeMPI() {
    try {
      // lets do a barrier here so everyone is synchronized at the end
      // commenting out barrier to fix stale workers issue
      // MPI.COMM_WORLD.barrier();
      if (JobMasterContext.isJobMasterUsed(config)) {
        closeWorker();
      }
      MPI.Finalize();
    } catch (MPIException ignore) {
    }
  }

  /**
   * Construct the MPIWorker starter
   *
   * @param args the main args
   */
  private MPIWorker(String[] args) {
    Options cmdOptions = null;
    try {
      MPI.InitThread(args, MPI.THREAD_MULTIPLE);
      int rank = MPI.COMM_WORLD.getRank();

      // on any uncaught exception, we will call MPI Finalize and exit
      Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
        LOG.log(Level.SEVERE, "Uncaught exception in thread "
            + thread + ". Finalizing this worker...", throwable);

        if (JobMasterContext.isJobMasterUsed(config)) {
          JMSenderToDriver senderToDriver = JMWorkerAgent.getJMWorkerAgent().getSenderToDriver();
          Exception exception = (Exception) throwable;
          JobExecutionState.WorkerJobState workerState =
              JobExecutionState.WorkerJobState.newBuilder()
                  .setFailure(true)
                  .setJobName(config.getStringValue(Context.JOB_ID))
                  .setWorkerMessage(JSONUtils.toJSONString(exception, Exception.class))
                  .build();
          senderToDriver.sendToDriver(workerState);
        } else {
          throw new RuntimeException("Worker faild with exception", throwable);
        }
        finalizeMPI();
      });

      cmdOptions = setupOptions();
      CommandLineParser parser = new DefaultParser();
      // parse the help options first.
      CommandLine cmd = parser.parse(cmdOptions, args);

      // load the configuration
      // we are loading the configuration for all the components
      this.config = loadConfigurations(cmd, rank);
      // normal worker
      LOG.log(Level.FINE, "A worker process is starting...");

      String jobId = MPIContext.jobId(config);
      String jobDescFile = JobUtils.getJobDescriptionFilePath(jobId, config);
      JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);

      /* // todo: adding checkpoint info to the config could be a way to get start from an arbitary
         // checkpoint. This is undecided at the moment.
      this.config = updateCheckpointInfo(config);
      */

      // lets split the comm
      if (JobMasterContext.isJobMasterUsed(config)) {
        if (!JobMasterContext.jobMasterRunsInClient(config)) {
          // lets broadcast the worker info
          // broadcast the port of jobmaster
          int color = rank == 0 ? 0 : 1;
          Intracomm comm = MPI.COMM_WORLD.split(color, rank);

          if (rank != 0) {
            wInfo = createWorkerInfo(config, comm.getRank(), job);
          } else {
            wInfo = createWorkerInfo(config, -1, job);
          }

          // init WorkerRuntime
          WorkerRuntime.init(config, job, wInfo, JobMasterAPI.WorkerState.STARTED);

          // lets broadcast the master information
          broadCastMasterInformation(rank);

          if (rank != 0) {
            startWorker(config, rank, comm, job);
          } else {
            startMaster(config, rank);
          }
        } else {
          wInfo = createWorkerInfo(config, MPI.COMM_WORLD.getRank(), job);
          WorkerRuntime.init(config, job, wInfo, JobMasterAPI.WorkerState.STARTED);
          startWorker(config, rank, MPI.COMM_WORLD, job);
        }
      } else {
        wInfo = createWorkerInfo(config, MPI.COMM_WORLD.getRank(), job);
        WorkerRuntime.init(config, job, wInfo, JobMasterAPI.WorkerState.STARTED);
        startWorkerWithoutMaster(config, rank, MPI.COMM_WORLD, job);
      }
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed the MPI process", e);
      throw new RuntimeException(e);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SubmitterMain", cmdOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.SEVERE, "Protocol buffer exception ", e);
    }

    finalizeMPI();
  }

  /**
   * Broadcast the master information to workers
   *
   * @param rank rank
   * @throws MPIException if an error occurs
   * @throws InvalidProtocolBufferException if an error occurs
   */
  private void broadCastMasterInformation(int rank) throws MPIException,
      InvalidProtocolBufferException {
    byte[] workerBytes = wInfo.toByteArray();
    int length = workerBytes.length;
    IntBuffer countSend = MPI.newIntBuffer(1);
    if (rank == 0) {
      countSend.put(length);
    }
    MPI.COMM_WORLD.bcast(countSend, 1, MPI.INT, 0);
    length = countSend.get(0);

    ByteBuffer sendBuffer = MPI.newByteBuffer(length);
    if (rank == 0) {
      sendBuffer.put(workerBytes);
    }
    MPI.COMM_WORLD.bcast(sendBuffer, length, MPI.BYTE, 0);
    byte[] c = new byte[length];
    if (rank != 0) {
      sendBuffer.get(c);
      JobMasterAPI.WorkerInfo masterInfo = JobMasterAPI.WorkerInfo.
          newBuilder().mergeFrom(c).build();
      config = Config.newBuilder().putAll(config).
          put(JobMasterContext.JOB_MASTER_PORT, masterInfo.getPort()).
          put(JobMasterContext.JOB_MASTER_IP, masterInfo.getNodeInfo().getNodeIP()).build();
    } else {
      config = Config.newBuilder().putAll(config).
          put(JobMasterContext.JOB_MASTER_PORT, wInfo.getPort()).
          put(JobMasterContext.JOB_MASTER_IP, wInfo.getNodeInfo().getNodeIP()).build();
    }
  }

  public static void main(String[] args) {
    new MPIWorker(args);
  }

  /**
   * Create the resource plan
   *
   * @return the worker controller
   */
  private IWorkerController createWorkerController(JobAPI.Job job) {
    // first get the worker id
    String jobMasterIP = JobMasterContext.jobMasterIP(config);
    int jobMasterPort = JobMasterContext.jobMasterPort(config);
    int numberOfWorkers = job.getNumberOfWorkers();

    this.masterClient = createMasterAgent(config, jobMasterIP, jobMasterPort,
        wInfo, numberOfWorkers);

    return masterClient.getJMWorkerController();
  }

  /**
   * Create the job master client to get information about the workers
   */
  private JMWorkerAgent createMasterAgent(Config cfg, String masterHost, int masterPort,
                                          JobMasterAPI.WorkerInfo workerInfo,
                                          int numberContainers) {

    // should be either WorkerState.STARTED or WorkerState.RESTARTED
    JobMasterAPI.WorkerState initialState = JobMasterAPI.WorkerState.STARTED;

    // we start the job master client
    JMWorkerAgent jobMasterAgent = JMWorkerAgent.createJMWorkerAgent(cfg,
        workerInfo, masterHost, masterPort, numberContainers, initialState);
    LOG.log(Level.FINE, String.format("Connecting to job master %s:%d", masterHost, masterPort));
    jobMasterAgent.startThreaded();

    return jobMasterAgent;
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

    Option jobId = Option.builder("j")
        .desc("Job Id")
        .longOpt("job_id")
        .hasArgs()
        .argName("job id")
        .required()
        .build();

    Option jobMasterIP = Option.builder("i")
        .desc("Job master ip")
        .longOpt("job_master_ip")
        .hasArgs()
        .argName("job master ip")
        .required()
        .build();

    Option jobMasterPort = Option.builder("p")
        .desc("Job master ip")
        .longOpt("job_master_port")
        .hasArgs()
        .argName("job master port")
        .required()
        .build();

    options.addOption(twister2Home);
    options.addOption(containerClass);
    options.addOption(configDirectory);
    options.addOption(clusterType);
    options.addOption(jobId);
    options.addOption(jobMasterIP);
    options.addOption(jobMasterPort);

    return options;
  }

  private Config loadConfigurations(CommandLine cmd, int id) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String container = cmd.getOptionValue("container_class");
    String configDir = cmd.getOptionValue("config_dir");
    String clusterType = cmd.getOptionValue("cluster_type");
    String jobId = cmd.getOptionValue("job_id");
    String jIp = cmd.getOptionValue("job_master_ip");
    int jPort = Integer.parseInt(cmd.getOptionValue("job_master_port"));

    LOG.log(Level.FINE, String.format("Initializing process with "
            + "twister_home: %s container_class: %s config_dir: %s cluster_type: %s",
        twister2Home, container, configDir, clusterType));

    Config cfg = ConfigLoader.loadConfig(twister2Home, configDir, clusterType);

    Config workerConfig = Config.newBuilder().putAll(cfg).
        put(MPIContext.TWISTER2_HOME.getKey(), twister2Home).
        put(MPIContext.WORKER_CLASS, container).
        put(MPIContext.TWISTER2_CONTAINER_ID, id).
        put(MPIContext.TWISTER2_CLUSTER_TYPE, clusterType).build();

    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobId, workerConfig);
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);

    Config updatedConfig = JobUtils.overrideConfigs(job, cfg);

    updatedConfig = Config.newBuilder().putAll(updatedConfig).
        put(MPIContext.TWISTER2_HOME.getKey(), twister2Home).
        put(MPIContext.WORKER_CLASS, container).
        put(MPIContext.TWISTER2_CONTAINER_ID, id).
        put(MPIContext.JOB_ID, jobId).
        put(MPIContext.JOB_OBJECT, job).
        put(MPIContext.TWISTER2_CLUSTER_TYPE, clusterType).
        put(JobMasterContext.JOB_MASTER_IP, jIp).
        put(JobMasterContext.JOB_MASTER_PORT, jPort).
        put(FaultToleranceContext.FAULT_TOLERANT, false).
        put(ZKContext.ZK_BASED_GROUP_MANAGEMENT, false).
        build();
    return updatedConfig;
  }

  /**
   * Start the master
   *
   * @param cfg configuration
   * @param rank mpi rank
   */
  private void startMaster(Config cfg, int rank) {
    // lets do a barrier here so everyone is synchronized at the start
    // lets create the resource plan
    JobAPI.Job job = (JobAPI.Job) cfg.get(MPIContext.JOB_OBJECT);

    try {
      int port = JobMasterContext.jobMasterPort(cfg);
      String hostAddress = InetAddress.getLocalHost().getHostAddress();
      LOG.log(Level.INFO, String.format("Starting the job manager: %s:%d", hostAddress, port));
      JobMasterAPI.NodeInfo jobMasterNodeInfo = null;
      IScalerPerCluster clusterScaler = null;
      JobMasterAPI.JobMasterState initialState = JobMasterAPI.JobMasterState.JM_STARTED;
      NomadTerminator nt = new NomadTerminator();

      JobMaster jobMaster = new JobMaster(
          cfg, hostAddress, port, nt, job, jobMasterNodeInfo, clusterScaler, initialState);
      jobMaster.addShutdownHook(false);
      Thread jmThread = jobMaster.startJobMasterThreaded();

      try {
        if (jmThread != null) {
          jmThread.join();
        }
      } catch (InterruptedException ignore) {
      }

      LOG.log(Level.INFO, "Master done... ");
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Exception when getting local host address: ", e);
      throw new RuntimeException(e);
    } catch (Twister2Exception e) {
      LOG.log(Level.SEVERE, "Exception when starting Job master: ", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Start the worker
   *
   * @param cfg configuration
   * @param rank global rank
   * @param intracomm communication
   */
  private void startWorker(Config cfg, int rank, Intracomm intracomm, JobAPI.Job job) {
    try {
      String twister2Home = Context.twister2Home(cfg);
      // initialize the logger
      initLogger(cfg, intracomm.getRank(), twister2Home);

      // now create the worker
//      IWorkerController wc = createWorkerController(job);
      IWorkerController wc = WorkerRuntime.getWorkerController();
      MPIJobWorkerController mpiWorkerContorller = new MPIJobWorkerController(wc);
      IPersistentVolume persistentVolume = initPersistenceVolume(cfg, job.getJobName(), rank);

      mpiWorkerContorller.add("comm", intracomm);
      String workerClass = MPIContext.workerClass(cfg);
      try {
        Object object = ReflectionUtils.newInstance(workerClass);
        if (object instanceof IWorker) {
          IWorker container = (IWorker) object;
          // now initialize the container
          container.execute(cfg, intracomm.getRank(), mpiWorkerContorller, persistentVolume,
              null);
        } else {
          throw new RuntimeException("Cannot instantiate class: " + object.getClass());
        }
        LOG.log(Level.FINE, "loaded worker class: " + workerClass);
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        LOG.log(Level.SEVERE, String.format("failed to load the worker class %s",
            workerClass), e);
        throw new RuntimeException(e);
      }

      LOG.log(Level.FINE, String.format("Worker %d: the cluster is ready...", rank));
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed to synchronize the workers at the start");
      throw new RuntimeException(e);
    }
  }

  /**
   * Start the worker
   *
   * @param cfg configuration
   * @param rank global rank
   * @param intracomm communication
   */
  private void startWorkerWithoutMaster(Config cfg, int rank, Intracomm intracomm, JobAPI.Job job) {
    try {
      String twister2Home = Context.twister2Home(cfg);
      // initialize the logger
      initLogger(cfg, intracomm.getRank(), twister2Home);

      Map<Integer, JobMasterAPI.WorkerInfo> infos = createResourcePlan(cfg, intracomm, job);
      MPIWorkerController wc = new MPIWorkerController(intracomm.getRank(), infos);
      IPersistentVolume persistentVolume = initPersistenceVolume(cfg, job.getJobName(), rank);

      // now create the worker
      wc.add("comm", intracomm);
      String workerClass = MPIContext.workerClass(cfg);
      try {
        Object object = ReflectionUtils.newInstance(workerClass);
        if (object instanceof IWorker) {
          IWorker container = (IWorker) object;
          // now initialize the container
          container.execute(cfg, intracomm.getRank(), wc, persistentVolume, null);
        } else {
          throw new RuntimeException("Cannot instantiate class: " + object.getClass());
        }
        LOG.log(Level.FINE, "loaded worker class: " + workerClass);
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        LOG.log(Level.SEVERE, String.format("failed to load the worker class %s",
            workerClass), e);
        throw new RuntimeException(e);
      }

      LOG.log(Level.FINE, String.format("Worker %d: the cluster is ready...", rank));
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed to synchronize the workers at the start");
      throw new RuntimeException(e);
    }
  }

  /**
   * last method to call to close the worker
   */
  private void closeWorker() {
    LOG.log(Level.INFO, String.format("Worker finished executing - %d", wInfo.getWorkerID()));
    // send worker completed message to the Job Master and finish
    // Job master will delete the StatefulSet object
    if (masterClient != null) {
      masterClient.sendWorkerCompletedMessage();
      masterClient.close();
    }
  }

  /**
   * create a AllocatedResources
   *
   * @param cfg configuration
   * @return a map of rank to hostname
   */
  public Map<Integer, JobMasterAPI.WorkerInfo> createResourcePlan(Config cfg,
                                                                  Intracomm intracomm,
                                                                  JobAPI.Job job) {
    try {
      JobMasterAPI.WorkerInfo workerInfo = createWorkerInfo(cfg, intracomm.getRank(), job);
      byte[] workerBytes = workerInfo.toByteArray();
      int length = workerBytes.length;

      IntBuffer countSend = MPI.newIntBuffer(1);
      int worldSize = intracomm.getSize();
      IntBuffer countReceive = MPI.newIntBuffer(worldSize);
      // now calculate the total number of characters
      countSend.put(length);
      intracomm.allGather(countSend, 1, MPI.INT, countReceive, 1, MPI.INT);

      int[] receiveSizes = new int[worldSize];
      int[] displacements = new int[worldSize];
      int sum = 0;
      for (int i = 0; i < worldSize; i++) {
        receiveSizes[i] = countReceive.get(i);
        displacements[i] = sum;
        sum += receiveSizes[i];
      }
      // now we need to send this to all the nodes
      ByteBuffer sendBuffer = MPI.newByteBuffer(length);
      ByteBuffer receiveBuffer = MPI.newByteBuffer(sum);
      sendBuffer.put(workerBytes);

      // now lets receive the process names of each rank
      intracomm.allGatherv(sendBuffer, length, MPI.BYTE, receiveBuffer,
          receiveSizes, displacements, MPI.BYTE);

      Map<Integer, JobMasterAPI.WorkerInfo> processNames = new HashMap<>();
      for (int i = 0; i < receiveSizes.length; i++) {
        byte[] c = new byte[receiveSizes[i]];
        receiveBuffer.get(c);
        JobMasterAPI.WorkerInfo info = JobMasterAPI.WorkerInfo.newBuilder().mergeFrom(c).build();
        processNames.put(i, info);
        LOG.log(Level.FINE, String.format("Process %d name: %s", i, processNames.get(i)));
      }
      return processNames;
    } catch (MPIException e) {
      throw new RuntimeException("Failed to communicate", e);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to create worker info", e);
    }
  }

  /**
   * Create worker information
   *
   * @param cfg configuration
   * @param workerId communicator
   * @param job job
   * @return the worker information
   * @throws MPIException if an error occurs
   */
  private JobMasterAPI.WorkerInfo createWorkerInfo(Config cfg, int workerId,
                                                   JobAPI.Job job) throws MPIException {
    String processName;
    try {
      processName = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Failed to get ip address", e);
    }

    JobMasterAPI.NodeInfo nodeInfo = NodeInfoUtils.createNodeInfo(processName,
        "default", "default");
    JobAPI.ComputeResource computeResource = JobUtils.getComputeResource(job, workerId);
    List<String> portNames = SchedulerContext.additionalPorts(cfg);
    final Map<String, Integer> freePorts = new HashMap<>();
    if (portNames == null) {
      portNames = new ArrayList<>();
    }
    portNames.add("__worker__");
    Map<String, ServerSocket> socketMap = NetworkUtils.findFreePorts(portNames);
    MPI.COMM_WORLD.barrier();
    AtomicBoolean closedSuccessfully = new AtomicBoolean(true);
    socketMap.forEach((k, v) -> {
      freePorts.put(k, v.getLocalPort());
      try {
        v.close();
      } catch (IOException e) {
        LOG.log(Level.SEVERE, e, () -> "Couldn't close opened server socket : " + k);
        closedSuccessfully.set(false);
      }
    });
    if (!closedSuccessfully.get()) {
      throw new IllegalStateException("Could not release one or more free TCP/IP ports");
    }
    Integer workerPort = freePorts.get("__worker__");
    freePorts.remove("__worker__");
    LOG.fine("Worker info host:" + processName + ":" + workerPort);
    return WorkerInfoUtils.createWorkerInfo(workerId,
        processName, workerPort, nodeInfo, computeResource, freePorts);
  }

  /**
   * Initialize the loggers to log into the task local directory
   *
   * @param cfg the configuration
   * @param workerID worker id
   */
  private void initLogger(Config cfg, int workerID, String logDirectory) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    // LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    // set logging level
    // LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cfg));

    String persistentJobDir;
    String jobWorkingDirectory = NomadContext.workingDirectory(cfg);
    String jobId = NomadContext.jobId(cfg);
    if (NomadContext.getLoggingSandbox(cfg)) {
      persistentJobDir = Paths.get(jobWorkingDirectory, jobId).toString();
    } else {
      persistentJobDir = logDirectory;
    }

    // if no persistent volume requested, return
    if (persistentJobDir == null) {
      return;
    }
    String logDir = persistentJobDir + "/logs/worker-" + workerID;
    File directory = new File(logDir);
    if (!directory.exists()) {
      if (!directory.mkdirs()) {
        throw new RuntimeException("Failed to create log directory: " + logDir);
      }
    }
    LoggingHelper.setupLogging(cfg, logDir, "worker-" + workerID);
    LOG.fine(String.format("Logging is setup with file %s", logDir));
  }


  private IPersistentVolume initPersistenceVolume(Config cfg, String jobId, int rank) {
    File baseDir = new File(MPIContext.fileSystemMount(cfg));

    // if the base dir does not exist
    while (!baseDir.exists() && !baseDir.mkdirs()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException("Thread interrupted", e);
      }
    }

    return new FSPersistentVolume(baseDir.getAbsolutePath(), rank);
  }

/* //todo: change this and include this in the checkpoint mgr
  private Config updateCheckpointInfo(Config cfg) {

    String checkpointDir = CheckpointingContext.checkpointDir(cfg);

    new File(checkpointDir).mkdirs();

    File checkpointFile = new File(checkpointDir + File.separator
        + CheckpointingContext.LAST_SNAPSHOT_FILE);

    long checkpointID = 0;
    if (checkpointFile.exists()) {
      try {
        checkpointID = Long.valueOf(new String(Files.readAllBytes(checkpointFile.toPath())).trim());
      } catch (IOException e) {
        throw new RuntimeException("Unable to read the checkpoint file", e);
      }
    }

    return Config.newBuilder()
        .putAll(cfg)
        .put(CheckpointingContext.RESTORE_SNAPSHOT, checkpointID)
        .build();
  }*/
}
