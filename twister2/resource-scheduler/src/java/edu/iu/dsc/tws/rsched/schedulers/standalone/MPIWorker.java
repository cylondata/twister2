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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.ConfigLoader;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.logging.LoggingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.resource.NodeInfoUtils;
import edu.iu.dsc.tws.common.resource.WorkerInfoUtils;
import edu.iu.dsc.tws.common.util.NetworkUtils;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.master.worker.JobMasterClient;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
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
  private JobMasterClient masterClient;

  /**
   * Configuration
   */
  private Config config;

  /**
   * Information of this worker
   */
  private JobMasterAPI.WorkerInfo wInfo;

  /**
   * Construct the MPIWorker starter
   * @param args the main args
   */
  private MPIWorker(String[] args) {
    Options cmdOptions = null;
    try {
      MPI.Init(args);
      int rank = MPI.COMM_WORLD.getRank();

      cmdOptions = setupOptions();
      CommandLineParser parser = new DefaultParser();
      // parse the help options first.
      CommandLine cmd = parser.parse(cmdOptions, args);

      // load the configuration
      // we are loading the configuration for all the components
      this.config = loadConfigurations(cmd, rank);
      // normal worker
      LOG.log(Level.FINE, "A worker process is starting...");

      String jobName = MPIContext.jobName(config);
      String jobDescFile = JobUtils.getJobDescriptionFilePath(jobName, config);
      JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);

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

          // lets broadcast the master information
          broadCastMasterInformation(rank);

          if (rank != 0) {
            startWorker(config, rank, comm, job);
            closeWorker();
          } else {
            startMaster(config, rank);
          }
        } else {
          wInfo = createWorkerInfo(config, MPI.COMM_WORLD.getRank(), job);
          startWorker(config, rank, MPI.COMM_WORLD, job);
          closeWorker();
        }
      } else {
        wInfo = createWorkerInfo(config, MPI.COMM_WORLD.getRank(), job);
        startWorkerWithoutMaster(config, rank, MPI.COMM_WORLD, job);
      }

      // lets do a barrier here so everyone is synchronized at the end
      MPI.COMM_WORLD.barrier();
    } catch (MPIException e) {
      LOG.log(Level.SEVERE, "Failed the MPI process", e);
      throw new RuntimeException(e);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SubmitterMain", cmdOptions);
      throw new RuntimeException("Error parsing command line options: ", e);
    } catch (Throwable t) {
      String msg = "Un-expected error";
      LOG.log(Level.SEVERE, msg, t);
      throw new RuntimeException(msg);
    } finally {
      try {
        MPI.Finalize();
      } catch (MPIException ignore) {
      }
    }
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
   * @return the worker controller
   */
  private IWorkerController createWorkerController(JobAPI.Job job) {
    // first get the worker id
    String jobMasterIP = JobMasterContext.jobMasterIP(config);
    int jobMasterPort = JobMasterContext.jobMasterPort(config);
    int numberOfWorkers = job.getNumberOfWorkers();

    this.masterClient = createMasterClient(config, jobMasterIP, jobMasterPort,
        wInfo, numberOfWorkers);

    return masterClient.getJMWorkerController();
  }

  /**
   * Create the job master client to get information about the workers
   */
  private JobMasterClient createMasterClient(Config cfg, String masterHost, int masterPort,
                                             JobMasterAPI.WorkerInfo workerInfo,
                                             int numberContainers) {
    // we start the job master client
    JobMasterClient jobMasterClient = JobMasterClient.createJobMasterClient(cfg,
        workerInfo, masterHost, masterPort, numberContainers);
    LOG.log(Level.INFO, String.format("Connecting to job master %s:%d", masterHost, masterPort));
    jobMasterClient.startThreaded();

    // now lets send the starting message
    jobMasterClient.sendWorkerRunningMessage();

    return jobMasterClient;
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
    options.addOption(jobName);
    options.addOption(jobMasterIP);
    options.addOption(jobMasterPort);

    return options;
  }

  private Config loadConfigurations(CommandLine cmd, int id) {
    String twister2Home = cmd.getOptionValue("twister2_home");
    String container = cmd.getOptionValue("container_class");
    String configDir = cmd.getOptionValue("config_dir");
    String clusterType = cmd.getOptionValue("cluster_type");
    String jobName = cmd.getOptionValue("job_name");
    String jIp = cmd.getOptionValue("job_master_ip");
    int jPort = Integer.parseInt(cmd.getOptionValue("job_master_port"));

    LOG.log(Level.FINE, String.format("Initializing process with "
        + "twister_home: %s container_class: %s config_dir: %s cluster_type: %s",
        twister2Home, container, configDir, clusterType));

    Config cfg = ConfigLoader.loadConfig(twister2Home, configDir + "/" + clusterType);

    Config workerConfig = Config.newBuilder().putAll(cfg).
        put(MPIContext.TWISTER2_HOME.getKey(), twister2Home).
        put(MPIContext.WORKER_CLASS, container).
        put(MPIContext.TWISTER2_CONTAINER_ID, id).
        put(MPIContext.TWISTER2_CLUSTER_TYPE, clusterType).build();

    String jobDescFile = JobUtils.getJobDescriptionFilePath(jobName, workerConfig);
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFile);

    Config updatedConfig = JobUtils.overrideConfigs(job, cfg);

    updatedConfig = Config.newBuilder().putAll(updatedConfig).
        put(MPIContext.TWISTER2_HOME.getKey(), twister2Home).
        put(MPIContext.WORKER_CLASS, container).
        put(MPIContext.TWISTER2_CONTAINER_ID, id).
        put(MPIContext.JOB_NAME, jobName).
        put(MPIContext.JOB_OBJECT, job).
        put(MPIContext.TWISTER2_CLUSTER_TYPE, clusterType).
        put(JobMasterContext.JOB_MASTER_IP, jIp).
        put(JobMasterContext.JOB_MASTER_PORT, jPort).build();
    return updatedConfig;
  }

  /**
   * Start the master
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
      JobMaster jobMaster =
          new JobMaster(cfg, hostAddress, port, new NomadTerminator(), job, jobMasterNodeInfo);
      jobMaster.addShutdownHook();
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
    }
  }

  /**
   * Start the worker
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
      IWorkerController wc = createWorkerController(job);
      MPIJobWorkerController mpiWorkerContorller = new MPIJobWorkerController(wc);
      mpiWorkerContorller.add("comm", intracomm);
      String workerClass = MPIContext.workerClass(cfg);
      try {
        Object object = ReflectionUtils.newInstance(workerClass);
        if (object instanceof IWorker) {
          IWorker container = (IWorker) object;
          // now initialize the container
          container.execute(cfg, intracomm.getRank(), mpiWorkerContorller, null, null);
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
      // now create the worker
      wc.add("comm", intracomm);
      String workerClass = MPIContext.workerClass(cfg);
      try {
        Object object = ReflectionUtils.newInstance(workerClass);
        if (object instanceof IWorker) {
          IWorker container = (IWorker) object;
          // now initialize the container
          container.execute(cfg, intracomm.getRank(), wc, null, null);
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
    LOG.log(Level.INFO, "Workers done... ");
    // send worker completed message to the Job Master and finish
    // Job master will delete the StatefulSet object
    masterClient.sendWorkerCompletedMessage();
    masterClient.close();
  }

  /**
   * create a AllocatedResources
   * @param cfg configuration
   * @return  a map of rank to hostname
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
    Map<String, Integer> freePorts = new HashMap<>();
    if (portNames == null) {
      portNames = new ArrayList<>();
    }
    portNames.add("__worker__");
    freePorts = NetworkUtils.findFreePorts(portNames);
    Integer workerPort = freePorts.get("__worker__");
    freePorts.remove("__worker__");

    return WorkerInfoUtils.createWorkerInfo(workerId,
        processName, workerPort, nodeInfo, computeResource, freePorts);
  }

  /**
   * Initialize the loggers to log into the task local directory
   * @param cfg the configuration
   * @param workerID worker id
   */
  private void initLogger(Config cfg, int workerID, String logDirectory) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    // set logging level
    LoggingHelper.setLogLevel(LoggingContext.loggingLevel(cfg));

    String persistentJobDir;
    String jobWorkingDirectory = NomadContext.workingDirectory(cfg);
    String jobName = NomadContext.jobName(cfg);
    if (NomadContext.getLoggingSandbox(cfg)) {
      persistentJobDir = Paths.get(jobWorkingDirectory, jobName).toString();
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
  }
}
