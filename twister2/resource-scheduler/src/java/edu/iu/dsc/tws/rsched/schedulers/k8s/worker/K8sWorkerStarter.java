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
package edu.iu.dsc.tws.rsched.schedulers.k8s.worker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.faulttolerance.FaultToleranceContext;
import edu.iu.dsc.tws.api.resource.IPersistentVolume;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.IWorkerStatusUpdater;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingContext;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;
import edu.iu.dsc.tws.rsched.core.WorkerRuntime;
import edu.iu.dsc.tws.rsched.schedulers.k8s.K8sEnvVariables;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import edu.iu.dsc.tws.rsched.worker.WorkerManager;
import static edu.iu.dsc.tws.api.config.Context.JOB_ARCHIVE_DIRECTORY;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.POD_MEMORY_VOLUME;

public final class K8sWorkerStarter {
  private static final Logger LOG = Logger.getLogger(K8sWorkerStarter.class.getName());

  private static Config config = null;
  private static int workerID = -1; // -1 means, not initialized
  private static JobMasterAPI.WorkerInfo workerInfo;
  private static String jobID = null;
  private static JobAPI.Job job = null;
  private static JobAPI.ComputeResource computeResource = null;

  // whether the worker is killed externally
  // if the worker is forcefully shutdown such as by scaling down
  // or by killing the job
  // we use shut down hook to clear some resources
  private static boolean externallyKilled = true;

  private K8sWorkerStarter() { }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    // all environment variables
    int workerPort = Integer.parseInt(System.getenv(K8sEnvVariables.WORKER_PORT + ""));
    String containerName = System.getenv(K8sEnvVariables.CONTAINER_NAME + "");
    String podName = System.getenv(K8sEnvVariables.POD_NAME + "");
    String hostIP = System.getenv(K8sEnvVariables.HOST_IP + "");
    String hostName = System.getenv(K8sEnvVariables.HOST_NAME + "");
    String jobMasterIP = System.getenv(K8sEnvVariables.JOB_MASTER_IP + "");
    String encodedNodeInfoList = System.getenv(K8sEnvVariables.ENCODED_NODE_INFO_LIST + "");
    jobID = System.getenv(K8sEnvVariables.JOB_ID + "");

    if (jobID == null) {
      throw new RuntimeException("JobID is null");
    }

    // load the configuration parameters from configuration directory
    String configDir = POD_MEMORY_VOLUME + "/" + JOB_ARCHIVE_DIRECTORY;

    config = K8sWorkerUtils.loadConfig(configDir);

    // read job description file
    String jobDescFileName = SchedulerContext.createJobDescriptionFileName(jobID);
    jobDescFileName = POD_MEMORY_VOLUME + "/" + JOB_ARCHIVE_DIRECTORY + "/" + jobDescFileName;
    job = JobUtils.readJobFile(null, jobDescFileName);
    LOG.info("Job description file is loaded: " + jobDescFileName);

    // add any configuration from job file to the config object
    // if there are the same config parameters in both,
    // job file configurations will override
    config = JobUtils.overrideConfigs(job, config);
    config = JobUtils.updateConfigs(job, config);

    // if there is no Driver in the job or checkpointing is not enabled,
    // and ZK is used for group management,
    // then, we don't need to connect to JM
    // if there is a driver or ZK is not used for group management, then we need to connect to JM
    if (!job.getDriverClassName().isEmpty()
        || !(ZKContext.isZooKeeperServerUsed(config))
        || CheckpointingContext.isCheckpointingEnabled(config)) {
      jobMasterIP = updateJobMasterIp(jobMasterIP);
    }

    // get podIP from localhost
    InetAddress localHost = null;
    try {
      localHost = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Cannot get localHost.", e);
    }

    String podIP = localHost.getHostAddress();
    JobMasterAPI.NodeInfo nodeInfo = KubernetesContext.nodeLocationsFromConfig(config)
        ? KubernetesContext.getNodeInfo(config, hostIP)
        : K8sWorkerUtils.getNodeInfoFromEncodedStr(encodedNodeInfoList, hostIP);

    LOG.info("PodName: " + podName + ", NodeInfo: " + nodeInfo);

    // set workerID
    workerID = K8sWorkerUtils.calculateWorkerID(job, podName, containerName);

    // get computeResource for this worker
    computeResource = K8sWorkerUtils.getComputeResource(job, podName);

    // generate additional ports if requested
    Map<String, Integer> additionalPorts =
        K8sWorkerUtils.generateAdditionalPorts(config, workerPort);

    // construct WorkerInfo
    workerInfo = WorkerInfoUtils.createWorkerInfo(
        workerID, podIP, workerPort, nodeInfo, computeResource, additionalPorts);

    // initialize persistent volume
    K8sPersistentVolume pv = null;
    if (KubernetesContext.persistentVolumeRequested(config)) {
      // create persistent volume object
      String persistentJobDir = KubernetesConstants.PERSISTENT_VOLUME_MOUNT;
      pv = new K8sPersistentVolume(persistentJobDir, workerID);
    }

    // initialize persistent logging
    K8sWorkerUtils.initWorkerLogger(workerID, pv, config);

    LOG.info("Worker information summary: \n"
        + "workerID: " + workerID + "\n"
        + "POD_IP: " + podIP + "\n"
        + "HOSTNAME(podname): " + podName + "\n"
        + "workerPort: " + workerPort + "\n"
        + "hostName(nodeName): " + hostName + "\n"
        + "hostIP(nodeIP): " + hostIP + "\n"
    );

    int restartCount = K8sWorkerUtils.getAndInitRestartCount(config, jobID, workerInfo);
    WorkerRuntime.init(config, job, workerInfo, restartCount);

    /**
     * Interfaces to interact with other workers and Job Master if there is any
     */
    IWorkerController workerController = WorkerRuntime.getWorkerController();
    IWorkerStatusUpdater workerStatusUpdater = WorkerRuntime.getWorkerStatusUpdater();

    // if this worker is restarted equal or more times than max restart config paramter,
    // finish up this worker
    if (restartCount >= FaultToleranceContext.maxRestarts(config)) {
      workerStatusUpdater.updateWorkerStatus(JobMasterAPI.WorkerState.FULLY_FAILED);
      WorkerRuntime.close();
      externallyKilled = false;
      return;
    }

    // add shut down hook
    addShutdownHook(workerStatusUpdater);

    // on any uncaught exception, we will label the worker as FAILED and throw a RuntimeException
    // JVM will be restarted by K8s
    Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
      LOG.log(Level.SEVERE, "Uncaught exception in the thread "
          + thread + ". Worker FAILED...", throwable);

      workerStatusUpdater.updateWorkerStatus(JobMasterAPI.WorkerState.FAILED);
      WorkerRuntime.close();
      externallyKilled = false;
      throw new RuntimeException("Worker failed with the exception", throwable);
    });

    // start the worker
    boolean completed = startWorker(workerController, pv);

    if (completed) {
      // update worker status to COMPLETED
      workerStatusUpdater.updateWorkerStatus(JobMasterAPI.WorkerState.COMPLETED);
    } else {
      // if not successfully completed, it means it is fully failed
      workerStatusUpdater.updateWorkerStatus(JobMasterAPI.WorkerState.FULLY_FAILED);
    }
    WorkerRuntime.close();
    externallyKilled = false;
  }

  /**
   * update jobMasterIP if necessary
   * if job master runs in client, jobMasterIP has to be provided as an environment variable
   * that variable must be provided as a parameter to this method
   * if job master runs as a separate pod,
   * we get the job master IP address from its pod
   * @param jobMasterIP
   */
  @SuppressWarnings("ParameterAssignment")
  public static String updateJobMasterIp(String jobMasterIP) {

    // if job master runs in client, jobMasterIP has to be provided as an environment variable
    if (JobMasterContext.jobMasterRunsInClient(config)) {
      if (jobMasterIP == null || jobMasterIP.trim().length() == 0) {
        throw new RuntimeException("Job master running in the client, but "
            + "this worker got job master IP as empty from environment variables.");
      }

      // get job master service ip from job master service name and use it as Job master IP
    } else {
      jobMasterIP = K8sWorkerUtils.getJobMasterServiceIP(
          KubernetesContext.namespace(config), jobID);

      // if it can not get jm ip by using service name
      // get it by watching jm pod
      if (jobMasterIP == null) {
        jobMasterIP = PodWatchUtils.getJobMasterIpByWatchingPodToRunning(
          KubernetesContext.namespace(config), jobID, 100);
        PodWatchUtils.close();
      }

      if (jobMasterIP == null) {
        throw new RuntimeException("Job master is running in a separate pod, but "
            + "this worker can not get the job master IP address from Kubernetes master.\n"
            + "Job master address: " + jobMasterIP);
      }
      LOG.info("Job master address: " + jobMasterIP);
    }

    // update config with jobMasterIP
    config = Config.newBuilder()
        .putAll(config)
        .put(JobMasterContext.JOB_MASTER_IP, jobMasterIP)
        .build();

    return jobMasterIP;
  }

  /**
   * start the Worker class specified in conf files
   */
  public static boolean startWorker(IWorkerController workerController,
                                 IPersistentVolume pv) {

    String workerClass = job.getWorkerClassName();
    IWorker worker;
    try {
      Object object = ReflectionUtils.newInstance(workerClass);
      worker = (IWorker) object;
      LOG.info("loaded worker class: " + workerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.severe(String.format("failed to load the worker class %s", workerClass));
      throw new RuntimeException(e);
    }

    K8sVolatileVolume volatileVolume = null;
    if (computeResource.getDiskGigaBytes() > 0) {
      volatileVolume = new K8sVolatileVolume(jobID, workerID);
    }
    WorkerManager workerManager = new WorkerManager(config, workerID,
        workerController, pv, volatileVolume, worker);
    return workerManager.execute();
  }

  /**
   * if the worker is killed by scaling down,
   * we need to let ZooKeeper know about it and delete worker znode
   * if zookeeper is used
   */
  public static void addShutdownHook(IWorkerStatusUpdater workerStatusUpdater) {

    Thread hookThread = new Thread() {
      public void run() {

        // if thw worker shuts down properly, do nothing
        if (!externallyKilled) {
          return;
        }

        workerStatusUpdater.updateWorkerStatus(JobMasterAPI.WorkerState.KILLED);
        WorkerRuntime.close();
      }
    };

    Runtime.getRuntime().addShutdownHook(hookThread);
  }

}
